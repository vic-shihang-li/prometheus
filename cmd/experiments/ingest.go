package main

import (
	"os"
	//"strings"
	"io/ioutil"
	//"time"
	"fmt"
	"path/filepath"
	//"math"
	"encoding/json"
	//"context"
	"math/rand"
	"sync"
	//"sync/atomic"
	"strconv"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/dterei/gotsc"
	plabels "github.com/prometheus/prometheus/pkg/labels"
)

type RawEntry struct {
	Timestamps []int64
	Values map[string][]float64
}

type Sample struct {
	time int64
	value float64
}

func load_univariate() [][]Sample {
	fmt.Println("Loading data")
	name := "bench1_univariate.json"
	content, err := ioutil.ReadFile(filepath.Join(DATAPATH, name))
	if err != nil {
		panic(err)
	}
	dict := make(map[string]RawEntry)
	err = json.Unmarshal(content, &dict)
	if err != nil {
		panic(err)
	}
	series := make([][]Sample, 0)
	for _, d := range dict {
		l := len(d.Timestamps)
		if l > MINSAMPLES {
			timeseries := d.Timestamps
			values, ok := d.Values["values"]
			if !ok {
				panic("No values data")
			}
			samples := make([]Sample, 0)
			for i := 0; i < l; i++ {
				ts := timeseries[i]
				v := float64(values[i])
				samples = append(samples, Sample {
					time: ts,
					value: v,
				})
			}
			series = append(series, samples)
		}
	}
	return series
}

//func make_univariate_samples(d DataEntry) []Data {
//	timeseries := d.Timestamps
//	values, ok := d.Values["values"]
//	if !ok {
//		panic("No values data")
//	}
//
//	data := make([]Data, 0)
//	for i := 0; i < len(timeseries); i++ {
//		d := Data {
//			time: timeseries[i],
//			value: float64(values[i]),
//		}
//		data = append(data, d)
//	}
//	return data
//}
//
//type Data struct {
//	time int64
//	value float64
//}

func ingest(ingest_id, nsrcs uint64, series [][]Sample, db *tsdb.DB, setup_gate, start_gate, done_gate *sync.WaitGroup, total_rate *float64, m *sync.Mutex) {

	data := make([][]Sample, 0);
	labels := make([]plabels.Labels, 0);
	for i := uint64(0); i < NSRCS; i++ {
		label := plabels.FromStrings(
			"ingest_id",
			strconv.Itoa(int(ingest_id)),
			"id",
			"id_" + strconv.Itoa(int(i)),
		)
		labels = append(labels, label)
		idx := rand.Intn(len(series) - 0);
		data = append(data, series[idx]);
	}

	done := make([]bool, nsrcs)
	refs := make([]uint64, nsrcs)
	done_count := uint64(0)
	float_count := 0

	tsc := gotsc.TSCOverhead()
	total_cycles := uint64(0);

	setup_gate.Done();
	start_gate.Wait();
	for done_count < nsrcs {
		appender := db.Appender(nil);
		for i := uint64(0); i < nsrcs; i++ {
			if !done[i] {
				item := data[i][0]
				label := labels[i]
				ref_id := refs[i]
				start := gotsc.BenchStart()
				ref, err := appender.Append(ref_id, label, item.time, item.value)
				if err != nil {
					panic(err)
				}
				end := gotsc.BenchEnd()
				total_cycles += end - start - tsc;
				float_count++
				if len(data[i]) == 1 {
					done_count += 1
					done[i] = true
				} else {
					data[i] = data[i][1:]
				}
				refs[i] = ref
			}
		}
		start := gotsc.BenchStart()
		err := appender.Commit()
		if err != nil {
			panic(err)
		}
		end := gotsc.BenchEnd()
		total_cycles += end - start - tsc;
	}
	dur := float64(total_cycles) / CYCLES_PER_SECOND;
	rate_per_second := float64(float_count) / dur;
	rate_mill_per_second := rate_per_second / 1000000.0;
	m.Lock();
	*total_rate += rate_mill_per_second;
	m.Unlock();
	done_gate.Done()
}

func run_ingest() {
	os.RemoveAll(PATH)

	data := load_univariate()
	// Load data (only univariate, control whether Synthetic or not using the SYNTH variable)
	nsrcs := uint64(10000)
	nscrapers := uint64(1)
	fmt.Println("******")
	fmt.Println("sources per scraper: ", nsrcs)
	fmt.Println("scrapers: ", nscrapers)

	// Setup tsdb
	opts := tsdb.DefaultOptions()
	opts.WALSegmentSize = -1

	// Force the head to not cut otherwise an out of bounds error occurs after the head is cut
	// and there are lagging writers. We want to ingest everything.
	// This sets min and max block duration to a year in millis. Data generated are in millis
	opts.MinBlockDuration = 31557600000
	opts.MaxBlockDuration = 31557600000
	opts.RetentionDuration = 31557600000

	db, err := tsdb.Open(PATH, nil, nil, opts, nil);
	if err != nil {
		panic(err)
	}

	// Run ingestion
	total_rate := 0.0;
	var m sync.Mutex;
	setup_gate := sync.WaitGroup{}
	start_gate := sync.WaitGroup{}
	start_gate.Add(1);
	done_gate := sync.WaitGroup{}

	for i := uint64(0); i < nscrapers; i++ {
		setup_gate.Add(1);
		done_gate.Add(1);
		go ingest(i, nsrcs, data, db, &setup_gate, &start_gate, &done_gate, &total_rate, &m)
	}
	// Wait for all to setup
	setup_gate.Wait();
	// Tell all to go
	start_gate.Done();
	// Wait for all to finish
	done_gate.Wait();
	fmt.Println("TOTAL RATE (million f64/s): ", total_rate)

	err = db.Close()
	if err != nil {
		panic(err)
	}

	walpath := PATH + "/wal"
	os.RemoveAll(walpath)
	os.Mkdir(walpath, 0755)
}



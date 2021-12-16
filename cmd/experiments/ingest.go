package main

import (
	"os"
	//"strings"
	"io/ioutil"
	"time"
	"fmt"
	"path/filepath"
	"math"
	"encoding/json"
	//"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"strconv"
	"github.com/dterei/gotsc"
	"github.com/prometheus/prometheus/tsdb"
	plabels "github.com/prometheus/prometheus/pkg/labels"
)

type DataEntry struct {
	Timestamps []int64
	Values map[string][]float64
}

func load_univariate() map[string][]Data {
	fmt.Println("Loading data")
	name := "bench1_univariate.json"
	if SYNTH {
		name = "synth_univariate.json"
	}
	content, err := ioutil.ReadFile(filepath.Join(DATAPATH, name))
	if err != nil {
		panic(err)
	}
	dict := make(map[string]DataEntry)
	err = json.Unmarshal(content, &dict)
	if err != nil {
		panic(err)
	}
	data := make(map[string][]Data)
	for k, d := range dict {
		d := make_univariate_samples(d)
		if len(d) > MINSAMPLES {
			data[k] = d
		}
	}
	return data
}

func make_univariate_samples(d DataEntry) []Data {
	timeseries := d.Timestamps
	values, ok := d.Values["values"]
	if !ok {
		panic("No values data")
	}

	data := make([]Data, 0)
	for i := 0; i < len(timeseries); i++ {
		d := Data {
			time: timeseries[i],
			value: float64(values[i]),
		}
		data = append(data, d)
	}
	return data
}

type Data struct {
	time int64
	value float64
}

func ingest(data_map map[uint64][]Data, ids []uint64, db *tsdb.DB, start_gate, done_gate, count_gate *sync.WaitGroup, total_count *uint64, total_rate *float64, m *sync.Mutex) {

	refs := make([]uint64, len(ids));
	done := make([]bool, len(ids))
	if done[0] {
		panic("initialized done to true")
	}
	labels := make([]plabels.Labels, 0)
	data := make([][]Data, 0)
	for _, id := range ids {
		label := plabels.FromStrings("id", "id_" + strconv.Itoa(int(id)))
		labels = append(labels, label)
		d, ok := data_map[id]
		if !ok {
			panic("CANT FIND ID")
		}
		data = append(data, d[:])
	}
	done_count := 0
	insert_count := uint64(0)

	start_gate.Wait();
	total_cycles := uint64(0);
	tsc := gotsc.TSCOverhead()
	for done_count < len(labels) {
		appender := db.Appender(nil);
		for i := 0; i < len(labels); i++ {
			if !done[i] {
				insert_count++
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

	done_gate.Done();
	atomic.AddUint64(total_count, uint64(insert_count));
	count_gate.Done();
	dur := float64(total_cycles) / (2693.672 * 1000000.0);
	//fmt.Printf("cycles: %d, seconds: %.6f\n", total_cycles, dur);
	//fmt.Println("Insert count: ", insert_count);
	rate_per_second := float64(insert_count) / dur;
	rate_mill_per_second := rate_per_second / 1000000.0;

	m.Lock();
	*total_rate += rate_mill_per_second;
	m.Unlock();

	//fmt.Printf("Thread rate (mfps): %.6f\n", rate_mill_per_second)
}

func ingest_setup(nsrcs, nscrapers uint64, data map[string][]Data, db *tsdb.DB) {

	datasets := make([][]Data, 0)
	dataset_names := make([]string, 0)
	for k, d := range data {
		datasets = append(datasets, d)
		dataset_names = append(dataset_names, k)
	}

	series_ids := make([][]uint64, nscrapers);
	data_map := make(map[uint64][]Data)
	name_map := make(map[uint64]string)
	for i := uint64(0); i < nsrcs; i++ {
		idx := i % nscrapers;
		series_ids[idx] = append(series_ids[idx], i)
		data_idx := rand.Intn(len(datasets))
		data_map[i] = datasets[data_idx]
		name_map[i] = dataset_names[data_idx]
	}
	DATA_MAP = data_map

	start_gate := sync.WaitGroup{}
	start_gate.Add(1);
	done_gate := sync.WaitGroup{}
	count_gate := sync.WaitGroup{}
	total_count := uint64(0);
	total_rate := 0.0;
	var m sync.Mutex;
	for _, ids := range series_ids {
		done_gate.Add(1)
		count_gate.Add(1)
		go ingest(data_map, ids, db, &start_gate, &done_gate, &count_gate, &total_count, &total_rate, &m)
	}

	start_gate.Done()
	start := time.Now()
	fmt.Println("Ingesting data")
	done_gate.Wait()
	elapsed := time.Since(start)
	count_gate.Wait()

	fmt.Println("Wrote", total_count, "floats in", elapsed);
	fmt.Println("Rate (million floats / second)", (float64(total_count)/elapsed.Seconds()) / 1000000);
	fmt.Println("TSC Rate (million floats/ second)", total_rate)

	fmt.Println("Wrinting range information")
	write_ranges("prom", data_map, name_map)
}

func run_ingest() {
	os.RemoveAll(PATH)

	// Load data (only univariate, control whether Synthetic or not using the SYNTH variable)
	data := load_univariate()
	for _, iteration := range []int{ 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		nsrcs := uint64(iteration * 10000)
		nscrapers := uint64(math.Min(float64(nsrcs), float64(NSCRAPERS)))
		//nscrapers = NSCRAPERS
		fmt.Println("NSRCS", nsrcs, "N Scrapers" , nscrapers, " sources per scraper", nsrcs/nscrapers)

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
		ingest_setup(nsrcs, nscrapers, data, db);

		// Compact head into a block for reads
		fmt.Println("NOT COMPACTING")
		//fmt.Println("Forced Head Compaction")
		//head := db.Head()
		//min := head.MinTime()
		//max := head.MaxTime()
		//db.CompactHead(tsdb.NewRangeHead(head, min, max))

		//db.Compact()
		err = db.Close()
		if err != nil {
			panic(err)
		}
	}

	walpath := PATH + "/wal"
	os.RemoveAll(walpath)
	os.Mkdir(walpath, 0755)
}



package main

import (
	"os"
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
	"github.com/prometheus/prometheus/tsdb"
	plabels "github.com/prometheus/prometheus/pkg/labels"
)

func load_univariate() [][]Data {
	fmt.Println("Loading data")
	name := "bench1_univariate.json"
	if SYNTH {
		name = "synth_univariate.json"
	}
	content, err := ioutil.ReadFile(filepath.Join(DATAPATH, name))
	if err != nil {
		panic(err)
	}
	dict := make(map[string]map[string][]float64)
	err = json.Unmarshal(content, &dict)
	if err != nil {
		panic(err)
	}
	data := make([][]Data, 0)
	for _, d := range dict {
		data = append(data, make_univariate_samples(d))
	}
	return data
}

func make_univariate_samples(d map[string][]float64) []Data {
	timeseries, ok := d["timestamps"]
	if !ok {
		panic("No timestamp data")
	}
	values, ok := d["values"]
	if !ok {
		panic("No timestamp data")
	}

	data := make([]Data, 0)
	for i := 0; i < len(timeseries); i++ {
		d := Data {
			time: int64(timeseries[i]),
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

func ingest(data_map map[uint64][]Data, ids []uint64, db *tsdb.DB, start_gate, done_gate, count_gate *sync.WaitGroup, total_count *uint64) {

	refs := make([]uint64, len(ids));
	done := make([]bool, len(ids))
	if done[0] {
		panic("initialized done to true")
	}
	labels := make([]plabels.Labels, 0)
	data := make([][]Data, 0)
	for _, id := range ids {
		label := plabels.FromStrings("__name__", "series", "id", "id_" + strconv.Itoa(int(id)))
		labels = append(labels, label)
		d, ok := data_map[id]
		if !ok {
			panic("CANT FIND ID")
		}
		data = append(data, d[:])
	}
	done_count := 0
	insert_count := 0

	start_gate.Wait();
	for done_count < len(labels) {
		appender := db.Appender(nil);
		for i := 0; i < len(labels); i++ {
			if !done[i] {
				insert_count++
				item := data[i][0]
				label := labels[i]
				ref_id := refs[i]
				ref, err := appender.Append(ref_id, label, item.time, item.value)
				if err != nil {
					panic(err)
				}
				if len(data[i]) == 1 {
					done_count += 1
					done[i] = true
				} else {
					data[i] = data[i][1:]
				}
				refs[i] = ref
			}
		}
		err := appender.Commit()
		if err != nil {
			panic(err)
		}
	}

	done_gate.Done();
	atomic.AddUint64(total_count, uint64(insert_count));
	count_gate.Done();



	//for _, item := range data {
	//	appender := db.Appender(nil);
	//	for i, id := range labels {
	//		ref, err := appender.Append(refs[i], id, item.time, item.value);
	//		if err != nil {
	//			panic(err)
	//		}
	//		refs[i] = ref
	//	}
	//	err := appender.Commit()
	//	if err != nil {
	//		panic(err)
	//	}
	//}

	//offsets := make([]int64, len(labels));
	//label_len := int64(len(labels))
	//data_len := int64(len(data))
	//z := NewZipfian(label_len, 0.99)
	//sequence := NewZipfianSamples(label_len, data_len, z)

	//for true {
	//	appender := db.Appender(nil);
	//	for i := 0; i < int(SAMPLES_PER_APPENDER); i++ {
	//		idx := sequence.NextItem();
	//		if idx == nil {
	//			break
	//		}
	//		refid := refs[*idx]
	//		label := labels[*idx]
	//		item := data[offsets[*idx]]
	//		ref, err := appender.Append(refid, label, item.time, item.value)
	//		if err != nil {
	//			panic(err)
	//		}
	//		refs[*idx] = ref
	//		offsets[*idx] += 1
	//	}
	//	err := appender.Commit()
	//	if err != nil {
	//		panic(err)
	//	}
	//}
}

func ingest_setup(nsrcs, nscrapers uint64, data [][]Data, db *tsdb.DB) {
	series_ids := make([][]uint64, nscrapers);
	data_map := make(map[uint64][]Data)
	for i := uint64(0); i < nsrcs; i++ {
		idx := i % nscrapers;
		series_ids[idx] = append(series_ids[idx], i)
		data_map[i] = data[rand.Intn(len(data))]
	}

	start_gate := sync.WaitGroup{}
	start_gate.Add(1);
	done_gate := sync.WaitGroup{}
	count_gate := sync.WaitGroup{}
	total_count := uint64(0);
	for _, ids := range series_ids {
		done_gate.Add(1)
		count_gate.Add(1)
		go ingest(data_map, ids, db, &start_gate, &done_gate, &count_gate, &total_count)
	}

	start_gate.Done()
	start := time.Now()
	fmt.Println("Ingesting data")
	done_gate.Wait()
	elapsed := time.Since(start)
	count_gate.Wait()

	fmt.Println("Wrote", total_count, "floats in", elapsed);
	fmt.Println("Rate (million floats / second)", (float64(total_count)/elapsed.Seconds()) / 1000000);
}

func run_ingest() {
	os.RemoveAll(PATH)

	// Load data (only univariate, control whether Synthetic or not using the SYNTH variable)
	data := load_univariate()
	nsrcs := NSRCS
	nscrapers := uint64(math.Min(float64(nsrcs), float64(NSCRAPERS)))
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
	fmt.Println("Forced Head Compaction")
	head := db.Head()
	min := head.MinTime()
	max := head.MaxTime()
	db.CompactHead(tsdb.NewRangeHead(head, min, max))

	//db.Compact()
	err = db.Close()
	if err != nil {
		panic(err)
	}

	walpath := PATH + "/wal"
	os.RemoveAll(walpath)
	os.Mkdir(walpath, 0755)
}



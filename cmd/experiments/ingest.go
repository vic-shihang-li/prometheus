package main

import (
	"os"
	"time"
	"fmt"
	"math"
	//"context"
	"math/rand"
	"sync"
	"strconv"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/pkg/labels"
)

type Data struct {
	time int64
	value float64
}

func ingest(data []Data, labels []labels.Labels, db *tsdb.DB, start_gate, done_gate *sync.WaitGroup) {
	defer done_gate.Done();
	refs := make([]uint64, len(labels));
	offsets := make([]int64, len(labels));

	label_len := int64(len(labels))
	data_len := int64(len(data))
	z := NewZipfian(label_len, 0.99)
	sequence := NewZipfianSamples(label_len, data_len, z)
	_ = sequence

	start_gate.Wait();
	for true {
		appender := db.Appender(nil);
		for i := 0; i < int(SAMPLES_PER_APPENDER); i++ {
			idx := sequence.NextItem();
			if idx == nil {
				break
			}
			refid := refs[*idx]
			label := labels[*idx]
			item := data[offsets[*idx]]
			ref, err := appender.Append(refid, label, item.time, item.value)
			if err != nil {
				panic(err)
			}
			refs[*idx] = ref
			offsets[*idx] += 1
		}
		err := appender.Commit()
		if err != nil {
			panic(err)
		}
	}
}

func ingest_setup(nsrcs, nscrapers uint64, data []Data, db *tsdb.DB) {
	series_ids := make([][]labels.Labels, nscrapers);
	for i := uint64(0); i < nsrcs; i++ {
		idx := i % nscrapers;
		l := labels.FromStrings("__name__", "series", "id", "id_" + strconv.Itoa(int(i)))
		series_ids[idx] = append(series_ids[idx], l);
	}

	start_gate := sync.WaitGroup{}
	start_gate.Add(1);
	done_gate := sync.WaitGroup{}
	for _, ids := range series_ids {
		done_gate.Add(1)
		go ingest(data, ids, db, &start_gate, &done_gate)
	}

	start_gate.Done()
	start := time.Now()
	fmt.Println("Ingesting data")
	done_gate.Wait()
	elapsed := time.Since(start)
	fmt.Println("Rate", (float64(uint64(len(data)) * nsrcs)/elapsed.Seconds()) / 1000000);
}

func random_float(min, max float64) float64 {
	return min + rand.Float64() * (max - min)
}

func gen_data(n uint64) []Data {
	data := make([]Data, n);
	state := 0.0;
	for i := 0; i < int(n); i++ {
		state = state + random_float(-1, 1);
		item := Data { int64(i) * int64(INTERVAL), state }
		data[i] = item
	}
	return data
}

func run_ingest() {
	path := PATH
	os.RemoveAll(PATH)
	nsrcs := NSRCS
	nscrapers := uint64(math.Min(float64(nsrcs), float64(NSCRAPERS)))
	nsamples := NSAMPLES
	total_floats := nsamples * nsrcs
	fmt.Println("N Samples" , nsamples, "NSRCS", nsrcs, "TOTAL", total_floats)
	fmt.Println("N Scrapers" , nscrapers, "; sources / scraper", nsrcs/nscrapers)

	_, err := os.Stat(path);
	if err == nil {
		err := os.RemoveAll(path)
		if err != nil {
			panic(err)
		}
	}

	data := gen_data(nsamples)
	fmt.Printf("first sample %+v\n", data[0])
	fmt.Printf("last sample %+v\n", data[len(data)-1])

	opts := tsdb.DefaultOptions()
	opts.WALSegmentSize = -1
	//opts.MinBlockDuration = nsamples
	//opts.MaxBlockDuration = nsamples
	//opts.AllowOverlappingBlocks = true
	db, err := tsdb.Open(path, nil, nil, opts, nil);
	if err != nil {
		panic(err)
	}
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

	walpath := path + "/wal"
	os.RemoveAll(walpath)
	os.Mkdir(walpath, 0755)
}



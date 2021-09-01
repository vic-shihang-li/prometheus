package main

import (
	"os"
	//"math"
	"context"
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var DATA_MAP map[uint64][]Data
var DATA map[string][]Data

type Interval struct {
	low  int64
	high int64
}

func query_runner(storage *tsdb.DB, totalSamples *uint64, setupGate, startGate, doneGate *sync.WaitGroup) {

	//storage, err := tsdb.OpenDBReadOnly(PATH, nil);
	//if err != nil {
	//	panic(err)
	//}

	zipf := NewZipfian(int64(len(DATA_MAP)), Q_ZIPF)
	_ = zipf

	keys := make([]uint64, 0)
	for k, _ := range DATA_MAP {
		keys = append(keys, k)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	queries := make([]map[string]Interval, 0)
	for i := 0; i < Q_NQUERIES; i++ {
		q := make(map[string]Interval)
		for len(q) < Q_NSRCS {
			key := keys[int(zipf.NextItem())]
			info, ok := DATA_MAP[key]
			if !ok {
				panic("No key")
			}
			high := info[len(info)-1].time
			low := info[len(info)-(rand.Intn(5000-1000)+1000)].time
			id := "id_" + strconv.Itoa(int(key))
			rng := Interval{low: low, high: high}
			q[id] = rng
		}
		queries = append(queries, q)
	}

	localSamples := uint64(0)
	setupGate.Done()
	startGate.Wait()
	for _, q := range queries {
		localSamples += query(q, storage)
	}
	doneGate.Done()
	atomic.AddUint64(totalSamples, localSamples)
}

func query(q map[string]Interval, storage *tsdb.DB) uint64 {

	totalSamples := uint64(0)
	done_gate := sync.WaitGroup{}
	for id, v := range q {
		m, err := labels.NewMatcher(labels.MatchEqual, "id", id)
		querier, err := storage.Querier(context.Background(), v.low, v.high)
		if err != nil {
			panic(err)
		}
		ss := querier.Select(false, nil, m)
		for ss.Next() {
			it := ss.At().Iterator()
			for it.Next() {
				totalSamples += 1
			}
		}
		querier.Close()
	}
	done_gate.Wait()
	return totalSamples
}

func run_get_range() {

	storage := setup_db()

	Q_NSRCS = 5000
	Q_THREADS = 32
	Q_NQUERIES = 10000 / Q_THREADS
	Q_ZIPF = 0.5
	fmt.Println("Q_NSRCS", i, "Q_THREADS", 1, "Q_NQUERIES", Q_NQUERIES)

	totalSamples := uint64(0)

	setup_gate := sync.WaitGroup{}
	start_gate := sync.WaitGroup{}
	start_gate.Add(1)
	done_gate := sync.WaitGroup{}

	fmt.Println("Launching threads")
	for i := 0; i < Q_THREADS; i++ {
		setup_gate.Add(1)
		done_gate.Add(1)
		go query_runner(storage, &totalSamples, &setup_gate, &start_gate, &done_gate)
	}
	fmt.Println("Waiting for threads to set up")
	setup_gate.Wait()
	fmt.Println("Starting all threads")
	start_gate.Done()
	start := time.Now()
	done_gate.Wait()
	dur := time.Since(start)
	fmt.Println("All threads done")

	fmt.Println(">>>")
	fmt.Println("Total samples read", totalSamples)
	fmt.Println("Total queries executed", Q_THREADS*Q_NQUERIES)
	fmt.Println("Total timeseries queried", Q_THREADS*Q_NQUERIES*Q_NSRCS)
	fmt.Println("Time Elapsed", dur)
	fmt.Println(">> Million Floats per second", (float64(totalSamples)/dur.Seconds())/1000000)
	storage.Close()
}

func setup_db() *tsdb.DB {
	start := time.Now()

	DATA = load_univariate()

	// Setup tsdb
	opts := tsdb.DefaultOptions()
	opts.WALSegmentSize = -1

	// Force the head to not cut otherwise an out of bounds error occurs after the head is cut
	// and there are lagging writers. We want to ingest everything.
	// This sets min and max block duration to a year in millis. Data generated are in millis
	opts.MinBlockDuration = 31557600000
	opts.MaxBlockDuration = 31557600000
	opts.RetentionDuration = 31557600000

	os.RemoveAll(PATH)
	db, err := tsdb.Open(PATH, nil, nil, opts, nil)
	if err != nil {
		panic(err)
	}

	// Run ingestion
	fmt.Println("Ingesting data")
	ingest_setup(uint64(100000), 72, DATA, db)
	fmt.Println("Loading data took", time.Since(start))
	fmt.Println("Loaded", len(DATA_MAP), "timeseries")
	return db
}

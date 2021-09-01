package main

import (
	"os"
	//"math"
	"context"
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/storage"
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

func query_runner(db *tsdb.DB, totalSamples *uint64, setupGate, startGate, doneGate *sync.WaitGroup, mint, maxt int64) {

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

	//queries := make([]map[string]Interval, 0)
	queries := make([][]*labels.Matcher, 0);
	for i := 0; i < Q_NQUERIES; i++ {
		q := make([]*labels.Matcher, 0)
		for len(q) < Q_NSRCS {
			key := keys[int(zipf.NextItem())]
			m, err := labels.NewMatcher(labels.MatchEqual, "id", "id_" + strconv.Itoa(int(key)))
			if err != nil {
				panic(err)
			}
			q = append(q, m)
			//high := max_time
			//low := min_time
			//id := "id_" + strconv.Itoa(int(key))
			//rng := Interval{low: low, high: high}
			//q[id] = rng
		}
		queries = append(queries, q)
	}

	querier, err := db.Querier(context.Background(), mint, maxt)
	if err != nil {
		panic(err)
	}

	localSamples := uint64(0)
	setupGate.Done()
	startGate.Wait()
	for _, q := range queries {
		localSamples += query(q, querier)
	}
	doneGate.Done()
	querier.Close()
	atomic.AddUint64(totalSamples, localSamples)
}

func query(q []*labels.Matcher, querier storage.Querier) uint64 {

	totalSamples := uint64(0)
	for _, m := range q {
		ss := querier.Select(false, nil, m)
		for ss.Next() {
			it := ss.At().Iterator()
			for it.Next() {
				totalSamples += 1
			}
		}
	}
	return totalSamples
}

func run_get_range() {

	storage := setup_db()

	// Find max time in data
	maxt := int64(0)
	for _, v := range DATA_MAP {
		last := v[len(v)-1].time
		if maxt < last {
			maxt = last
		}
	}
	// And calculate the min time to query
	//lo := 1000 * 60 * 30 // data are in millis, 30 minutes
	hi := 1000 * 60 * 60 * 1
	mint := maxt - int64(hi) //int64(rand.Intn(hi-lo) + lo)

	for _, q_threads := range []uint64{1, 4, 8, 16, 32, 40, 48, 56, 64, 72} {
		Q_NSRCS = 1
		Q_THREADS = int(q_threads)
		Q_NQUERIES = 100000
		Q_ZIPF = 0.5
		fmt.Println(">>>")
		fmt.Println("Q_NSRCS", Q_NSRCS, "Q_THREADS", Q_THREADS, "Q_NQUERIES", Q_NQUERIES)

		totalSamples := uint64(0)

		setup_gate := sync.WaitGroup{}
		start_gate := sync.WaitGroup{}
		start_gate.Add(1)
		done_gate := sync.WaitGroup{}

		fmt.Println("Launching threads")
		for i := 0; i < Q_THREADS; i++ {
			setup_gate.Add(1)
			done_gate.Add(1)
			go query_runner(storage, &totalSamples, &setup_gate, &start_gate, &done_gate, mint, maxt)
		}
		fmt.Println("Waiting for threads to set up")
		setup_gate.Wait()
		fmt.Println("Starting all threads")
		start_gate.Done()
		start := time.Now()
		done_gate.Wait()
		dur := time.Since(start)
		fmt.Println("All threads done")

		fmt.Println("Total samples read", totalSamples)
		fmt.Println("Total queries executed", Q_THREADS*Q_NQUERIES)
		fmt.Println("Total timeseries queried", Q_THREADS*Q_NQUERIES*Q_NSRCS)
		fmt.Println("Time Elapsed", dur)
		fmt.Println(">> Million Floats per second", (float64(totalSamples)/dur.Seconds())/1000000)
	}
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

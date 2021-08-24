package main

import (
	"os"
	"math"
	"time"
	"fmt"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"strconv"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/pkg/labels"
)

type Interval struct {
	low int64
	high int64
}

func query_runner(info map[uint64]RangeInfo, storage *tsdb.DBReadOnly, totalSamples *uint64, setupGate, startGate, doneGate *sync.WaitGroup){

	storage, err := tsdb.OpenDBReadOnly(PATH, nil);
	if err != nil {
		panic(err)
	}

	zipf := NewZipfian(int64(len(info)), Q_ZIPF)
	_ = zipf

	keys := make([]uint64, 0)
	for k, _ := range info {
		keys = append(keys, k)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	queries := make([]map[string]Interval, 0)
	for i := 0; i < Q_NQUERIES; i++ {
		q := make(map[string]Interval)
		for j := 0; j < Q_NSRCS; j++ {
			key := keys[int(zipf.NextItem())]
			inf, ok := info[key]
			if !ok {
				panic("No key")
			}
			id := "id_" + strconv.Itoa(int(key))
			rng := Interval { low: inf.TsQuery(), high: inf.TsLast }
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

func query(q map[string]Interval, storage *tsdb.DBReadOnly) uint64 {

	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)
	matchers := make([]*labels.Matcher, len(q))
	idx := 0
	for id, v := range q {
		if mint > v.low {
			mint = v.low
		}
		if maxt < v.high {
			maxt = v.high
		}
		m, err := labels.NewMatcher(labels.MatchEqual, "id", id)
		if err != nil {
			panic(err)
		}
		matchers[idx] = m
		idx += 1
	}

	querier, err := storage.Querier(context.Background(), mint, maxt)
	if err != nil {
		panic(err)
	}

	totalSamples := uint64(0)

	done_gate := sync.WaitGroup{};
	for _, matcher := range matchers {
		go func() {
			ss := querier.Select(false, nil, matcher)
			localSamples := uint64(0)
			for ss.Next() {
				it := ss.At().Iterator()
				for it.Next() {
					localSamples += 1
				}
			}
			atomic.AddUint64(&totalSamples, localSamples)
		}()
	}
	querier.Close()
	done_gate.Wait()
	return totalSamples
}

func run_get_range() {

	_, err := os.Stat(PATH);
	if err != nil {
		panic(err)
	}

	fmt.Println("Setting up")
	storage, err := tsdb.OpenDBReadOnly(PATH, nil);
	if err != nil {
		panic(err)
	}

	totalSamples := uint64(0)

	fmt.Println("Loading range information")
	ranges := read_ranges("prom")
	setup_gate := sync.WaitGroup{};
	start_gate := sync.WaitGroup{};
	start_gate.Add(1)
	done_gate := sync.WaitGroup{};

	fmt.Println("Launching threads")
	for i := 0; i < Q_THREADS; i++ {
		setup_gate.Add(1)
		done_gate.Add(1)
		go query_runner(ranges, storage, &totalSamples, &setup_gate, &start_gate, &done_gate)
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
	fmt.Println("Total queries executed", Q_THREADS * Q_NQUERIES)
	fmt.Println("Total timeseries queried", Q_THREADS * Q_NQUERIES * Q_NSRCS)
	fmt.Println("Time Elapsed", dur)
	storage.Close()
}



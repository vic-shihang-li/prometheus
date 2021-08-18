package main

import (
	"os"
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

func get_labels() []string {
	set := make(map[int]bool)
	for len(set) < int(Q_NSRCS) {
		set[rand.Intn(int(NSRCS))] = true
	}

	data := make([]string, 0)
	for k, _ := range set {
		data = append(data, "id_" + strconv.Itoa(k))
	}

	fmt.Println("Querying", len(data), "sources")
	return data
}

func run_get_range() {

	_, err := os.Stat(PATH);
	if err != nil {
		panic(err)
	}

	storage, err := tsdb.OpenDBReadOnly(PATH, nil);
	if err != nil {
		panic(err)
	}
	//storage.FlushWAL(PATH + "/wal")
	//walpath := path + "/wal"
	//os.RemoveAll(walpath)
	//os.Mkdir(walpath, 0755)

	matchers := make([]*labels.Matcher, 0)

	for _, id := range get_labels() {
		m, err := labels.NewMatcher(labels.MatchEqual, "id", id)
		if err != nil {
			panic(err)
		}
		matchers = append(matchers, m)
	}
	//_ = get_labels()
	//m, err := labels.NewMatcher(labels.MatchEqual, "id", "id_123")
	//matchers = append(matchers, m)
	q, err := storage.Querier(context.Background(), 98999, 99999)
	fmt.Println("QUERYING")

	totalSamples := uint64(0)
	//start_gate := sync.WaitGroup{};
	//start_gate.Add(1)
	done_gate := sync.WaitGroup{};
	start := time.Now()
	for _, matcher := range matchers {
		done_gate.Add(1)
		go func() {
			localSamples := uint64(0)
			ss := q.Select(false, nil, matcher)
			//start_gate.Wait()
			for ss.Next() {
				it := ss.At().Iterator()
				for it.Next() {
					localSamples = localSamples + 1
				}
			}
			atomic.AddUint64(&totalSamples, localSamples)
			done_gate.Done()
		}()
	}
	done_gate.Wait()
	fmt.Println("Time Elapsed", time.Since(start))
	fmt.Println("samples read", totalSamples)
	q.Close()
	storage.Close()
}



package main

import (
	"context"
	//"os"
	"fmt"
	"strconv"
	//"strings"
	//"testing"
	"time"
	"math/rand"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/promql"
	//"github.com/prometheus/prometheus/pkg/labels"
	//"github.com/prometheus/prometheus/promql/parser"
)

func random_ids(max, nids int) string {
	id_set := make(map[int]bool)
	for true {
		id := rand.Intn(max)
		id_set[id] = true
		if len(id_set) == nids {
			break
		}
	}
	fmt.Println(len(id_set))

	query_ids := ""
	l := 0
	for k, _ := range id_set {
		query_ids = query_ids + "id_" + strconv.Itoa(k)
		if l < nids-1 {
			query_ids = query_ids + "|"
		}
		l += 1
	}

	return query_ids
}

func setup() {
	//path := "/data/fsolleza/data/prometheus-query"
	//path := "/data/prometheus-query"
	path := "/hot/scratch/franco/prometheus-query"
	storage, err := tsdb.OpenDBReadOnly(path, nil);
	if err != nil {
		panic(err)
	}

	eng_opts := promql.EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 50000000,
		Timeout:    100 * time.Second,
	}
	engine := promql.NewEngine(eng_opts)


	origin := time.Unix(0, 0)
	n_ids := 100000
	n_srcs_to_query := 1000
	n_samples := 100000
	n_lookback := 1000
	rate := time.Millisecond
	window_stride := 3
	window_size := 3

	query_ids := random_ids(n_ids, n_srcs_to_query)
	end := origin.Add(rate * time.Duration(n_samples-1))
	start := end.Add(-rate * time.Duration(n_lookback))

	fmt.Println("Querying")
	q := fmt.Sprintf("max(min_over_time(series{id=~\"%s\"}[%dms]))", query_ids, window_size)
	//fmt.Println(q)
	timer_start := time.Now()
	qry, err := engine.NewRangeQuery(storage, q, start, end, time.Duration(window_stride) * rate)
	fmt.Println("Query setup done. Time taken: ", time.Since(timer_start))
	if err != nil {
		panic(err)
	}
	fmt.Println("Query execution")
	timer_start = time.Now()
	res := qry.Exec(context.Background())
	if res.Err != nil {
		panic(res.Err)
	}
	fmt.Println("Query execution done. Time taken: ", time.Since(timer_start))
	//fmt.Println(res.String())
	fmt.Println(qry.Stats())
	qry.Close()
}

func main() {
	setup()
}


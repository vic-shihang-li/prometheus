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

const double_group_by = false
const n_ids = 100000
const n_srcs_to_query = 10000
const n_samples = 100000
const n_lookback = 1000
const rate = time.Second
const window_stride = 3
const window_size = 3
const path = "/data/fsolleza/data/prometheus-query"
const double_group_by_query = "max(max_over_time(series{id=~\"%s\"}[%dms]))"
const single_group_by_query = "max_over_time(series{id=~\"%s\"}[%dms])"

func run() {
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
	query_ids := random_ids(n_ids, n_srcs_to_query)
	end := origin.Add(rate * time.Duration(n_samples-1))
	start := end.Add(-rate * time.Duration(n_lookback))

	fmt.Println("Querying")
	fmt.Println("Time range", start, end)
	qstr := single_group_by_query;
	if double_group_by {
		qstr = double_group_by_query
	}

	q := fmt.Sprintf(qstr, query_ids, window_size)
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
	if double_group_by {
		fmt.Println("Double group by")
	} else {
		fmt.Println("Single group by")
	}
	run()
}


package main

import (
	"context"
	//"os"
	//"fmt"
	//"strconv"
	//"strings"
	//"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/promql"
	//"github.com/prometheus/prometheus/pkg/labels"
	//"github.com/prometheus/prometheus/promql/parser"
)

func setup() {
	path := "/data/fsolleza/data/prometheus-query"
	storage, err := tsdb.OpenDBReadOnly(path, nil);
	if err != nil {
		panic(err)
	}

	origin := time.Unix(0, 0)
	end := origin.Add(time.Millisecond * 100000)
	start := end.Add(time.Millisecond * 1000)
	println("Origin", origin.UTC().String());
	println("Start", start.UTC().String());
	println("End", end.UTC().String());

	eng_opts := promql.EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 50000000,
		Timeout:    100 * time.Second,
	}
	engine := promql.NewEngine(eng_opts)

	qry, err := engine.NewInstantQuery(storage, "series{id=\"id_0\"}", end);
	if err != nil {
		panic(err)
	}
	res := qry.Exec(context.Background())
	if res.Err != nil {
		panic(res.Err)
	}
	println(res.Value.String())
	qry.Close()


	//q := "max(max_over_time(series{id=\"id_0\"}[20s]))"
	//q := "series"
	//qry, err := engine.NewRangeQuery(storage, q, start, end, bucket)
	//if err != nil {
	//	panic(err)
	//}
	//res := qry.Exec(context.Background())
	//if res.Err != nil {
	//	panic(res.Err)
	//}

	//println(res.Value.String())
	//qry.Close()
}

func main() {
	setup()
}


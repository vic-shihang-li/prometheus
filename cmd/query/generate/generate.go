package main

import (
	"os"
	"time"
	"fmt"
	"math"
	"context"
	"math/rand"
	"sync"
	"strconv"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/pkg/labels"
	//"github.com/prometheus/prometheus/promql"
)

type Data struct {
	time int64
	value float64
}

type Write struct {
	ref uint64
	data Data
}

type AppendInfo struct {
	app storage.Appender
	refs []uint64
	tx chan<- Write
	rx <-chan Write
}

func ingest(data []Data, labels []labels.Labels, db *tsdb.DB, start_gate, done_gate *sync.WaitGroup) {
	defer done_gate.Done();
	refs := make([]uint64, len(labels));
	start_gate.Wait();
	for _, item := range data {
		appender := db.Appender(context.Background());
		for i, id := range labels {
			ref, err := appender.Append(refs[i], id, item.time, item.value);
			if err != nil {
				panic(err)
			}
			refs[i] = ref
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
	done_gate.Wait()
}

func random_float(min, max float64) float64 {
	return min + rand.Float64() * (max - min)
}

func gen_data(n uint64) []Data {
	data := make([]Data, n);
	state := 0.0;
	for i := 0; i < int(n); i++ {
		state = state + random_float(-1, 1);
		item := Data { int64(i * 1000), state } // each sample is a second
		data[i] = item
	}
	return data
}

func main() {
	path := "/data/fsolleza/data/prometheus-query"
	//path := "/data/prometheus-query"
	//path := "/hot/scratch/franco/prometheus-query"
	os.RemoveAll(path)
	nsrcs := uint64(100000)
	nscrapers := uint64(math.Min(float64(nsrcs), 1))
	nsamples := uint64(100000)
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
	fmt.Printf("%+v\n", data[0])
	fmt.Printf("%+v\n", data[len(data)-1])
	println("Max timestamp: ", time.Unix(0, 0).UTC().Add(time.Second * 100000).String())

	opts := tsdb.DefaultOptions()
	//opts.WALSegmentSize = -1
	db, err := tsdb.Open(path, nil, nil, opts, nil);
	if err != nil {
		panic(err)
	}
	ingest_setup(nsrcs, nscrapers, data, db);

	db.Compact()
	err = db.Close()
	if err != nil {
		panic(err)
	}

	read, err := tsdb.OpenDBReadOnly(path, nil);
	read.FlushWAL(path)

	// Do some WAL cleanup to save disc space
	walpath := path + "/wal"
	os.RemoveAll(walpath)
	os.Mkdir(walpath, 0755)
}


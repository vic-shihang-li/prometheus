package main

import (
//	"time"
//	"fmt"
//	"math"
	"context"
//	"math/rand"
//	"sync"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/pkg/labels"
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

func ingest(data []Data, labels []labels.Labels, db *tsdb.DB) {
	refs := make([]uint64, len(labels));
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

func main() {
}

//func initialize(n, nsrcs, threads int, head *tsdb.Head) []AppendInfo {
//	appenders := make([]AppendInfo, threads)
//	srcs_per_chan := int(math.Ceil(float64(nsrcs)/float64(threads)))
//	fmt.Println("srcs per chan", srcs_per_chan);
//	for i:=0; i<threads; i++ {
//		appenders[i].app = head.Appender(context.Background())
//		ch := make(chan Write, n * srcs_per_chan)
//		appenders[i].tx = ch
//		appenders[i].rx = ch
//	}
//
//	for i:=0; i<nsrcs; i++ {
//		label := labels.FromStrings("num", string(i));
//		app := appenders[i%threads].app
//		refs := appenders[i%threads].refs
//		ref, _ := app.Append(0, label, 0, 0.0)
//		refs = append(refs, ref)
//		appenders[i%threads].refs = refs
//	}
//	return appenders
//}
//
//func gen_data(n int) []Data {
//	data := make([]Data, n);
//	for i := 0; i < int(n); i++ {
//		item := Data { int64(i), rand.Float64() }
//		data[i] = item
//	}
//	return data
//}
//
//func load_src(n int, ref uint64, tx chan<- Write, wg *sync.WaitGroup) {
//	for i := 0; i < int(n); i++ {
//		item := Data { int64(i), rand.Float64() }
//		tx <- Write { ref, item }
//	}
//	wg.Done()
//}
//
//func load_all(n int, appenders []AppendInfo) {
//	wg := sync.WaitGroup{}
//	for i:=0; i<len(appenders); i++ {
//		appender := appenders[i]
//		for j:=0; j<len(appender.refs); j++ {
//			ref := appender.refs[j]
//			wg.Add(1)
//			go load_src(n, ref, appender.tx, &wg)
//		}
//	}
//	wg.Wait()
//}
//
////var atmvar int32
//func run_appender(append_info AppendInfo, sync, wg *sync.WaitGroup) {
//	app := append_info.app
//	rx := append_info.rx
//	tx := append_info.tx
//	close(tx)
//
//	sync.Wait()
//	for item := range rx {
//		app.Append(item.ref, nil, item.data.time, item.data.value)
//		//atomic.AddInt32(&atmvar, 1)
//	}
//	app.Commit()
//	wg.Done()
//}
//
//func run_appender(head: 
//
//func main() {
//
//	nsrcs := 1
//	routines := 72
//	n := 100000000
//	total_floats := n * nsrcs
//	fmt.Println("N" , n, "NSRCS", nsrcs, "TOTAL", total_floats)
//
//	opts := tsdb.DefaultHeadOptions()
//	opts.ChunkDirRoot = "temp/"
//	head, _ := tsdb.NewHead(nil, nil, nil, opts)
//	app := head.Appender(context.Background()); // Initialized with an initAppender
//	app.Append(0, nil, 0, 0.0)
//
//	fmt.Println("Initializing")
//	appenders := initialize(n, nsrcs, routines, head)
//	fmt.Println("Loading")
//	load_all(n, appenders)
//
//	wg := sync.WaitGroup{}
//	sync := sync.WaitGroup{}
//	sync.Add(1)
//
//	fmt.Println("Setting up")
//	for i:=0; i<len(appenders); i++ {
//		wg.Add(1)
//		go run_appender(appenders[i], &sync, &wg)
//	}
//
//	start := time.Now()
//	sync.Done() // have all ingestion workers go at the same time!
//	fmt.Println("Ingesting data")
//	wg.Wait() // wait until all workers are done
//	elapsed := time.Since(start)
//	fmt.Println("Rate", (float64(n*nsrcs)/elapsed.Seconds()) / 1000000);
//	//fmt.Println("Total loaded: ", atmvar);
//}

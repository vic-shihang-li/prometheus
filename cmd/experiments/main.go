package main

import (
	"path/filepath"
	"encoding/json"
	"io/ioutil"
	"io/fs"
)

var PATH = "/data/fsolleza/data/prometheus"
var DATAPATH = "/data/fsolleza/data/data"
var TMPPATH = "/data/fsolleza/data/tmp"

var NSRCS = uint64(100000)
var NSCRAPERS = 32
var SYNTH = false
var MINSAMPLES = 30000

// Number of querying threads
var Q_THREADS = 32

// Zipf skew for queries w/in each thread
var Q_ZIPF = 0.99

// Number of queries per thread
var Q_NQUERIES = 1000

// Number of srcs to lookup per query
var Q_NSRCS = 1

// Determines the time range (1000, 5000, 10000)
var Q_NSAMPLES = 1000

func main() {
	run_ingest()
	//run_get_range()
}

type RangeInfo struct {
	Id uint64 `json: id`
	Dataset_name string
	Nsamples int
	TsFirst int64
	TsLast int64
	Ts1k int64
	Ts5k int64
	Ts10k int64
}

func (r *RangeInfo) TsQuery() int64 {
	if Q_NSAMPLES == 1000 {
		return r.Ts1k
	} else if Q_NSAMPLES == 5000 {
		return r.Ts5k
	} else if Q_NSAMPLES == 10000 {
		return r.Ts10k
	} else {
		panic("Q_NSAMPLES invalid")
	}
}

func read_ranges(prefix string) map[uint64]RangeInfo {
	name := prefix + "-range-info.json"
	content, err := ioutil.ReadFile(filepath.Join(TMPPATH, name))
	if err != nil {
		panic(err)
	}
	dict := make(map[uint64]RangeInfo)
	err = json.Unmarshal(content, &dict)
	if err != nil {
		panic(err)
	}
	return dict
}

func write_ranges(prefix string, data_map map[uint64][]Data, name_map map[uint64]string) {
	ranges := make(map[uint64]RangeInfo)
	for k, d := range data_map {
		dataset_name, ok := name_map[k];
		if !ok {
			panic("name not in map")
		}
		info := RangeInfo {
			Id: k,
			Dataset_name: dataset_name,
			Nsamples: len(d),
			TsFirst: d[0].time,
			TsLast: d[len(d)-1].time,
			Ts1k: d[len(d)-1000].time,
			Ts10k: d[len(d)-10000].time,
			Ts5k: d[len(d)-5000].time,
		}
		ranges[k] = info;
	}

	towrite, err := json.MarshalIndent(ranges, "", "  ")
	if err != nil {
		panic(err)
	}
	name := prefix + "-range-info.json"
	ioutil.WriteFile(filepath.Join(TMPPATH, name), towrite, fs.ModePerm)
}


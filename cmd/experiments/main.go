package main

var PATH = "/data/fsolleza/data/prometheus"
var NSRCS = uint64(1000)
var NSCRAPERS = 32
var NSAMPLES = uint64(100000)

var INTERVAL = 1 // in milliseconds

var Q_NSRCS = uint64(100)
var Q_SAMPLES = uint64(1000)

func main() {
	run_ingest()
	run_get_range()
}


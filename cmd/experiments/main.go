package main

var PATH = "/data/fsolleza/data/prometheus"
var NSRCS = uint64(10000)
var NSCRAPERS = 32
var NSAMPLES = uint64(10000)
var SAMPLES_PER_APPENDER = NSRCS // how many samples the appender should receive before committing()

var INTERVAL = 1 // in milliseconds

var Q_NSRCS = uint64(100)
var Q_MINT = 98999
var Q_MAXT = 99999

func main() {
	run_ingest()
	//run_get_range()
}


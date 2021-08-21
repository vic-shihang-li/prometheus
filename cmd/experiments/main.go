package main

var PATH = "/data/fsolleza/data/prometheus"
var DATAPATH = "/data/fsolleza/data/data"
var NSRCS = uint64(1000)
var NSCRAPERS = 32
var SYNTH = false

var Q_NSRCS = uint64(100)
var Q_MINT = 98999
var Q_MAXT = 99999

func main() {
	run_ingest()
	//run_get_range()
}


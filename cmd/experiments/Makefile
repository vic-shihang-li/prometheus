build:
	#go build -o main.bin main.go ingest.go get_range2.go zipfian.go
	go build -o main.bin main.go ingest.go
run:
	./main.bin
flamegraph: build
	mkdir -p tmp
	sudo perf record -F 200 -a -g -o tmp/perf.data ./main.bin
	sudo perf script -i tmp/perf.data | ~/Sandbox/FlameGraph/stackcollapse-perf.pl > tmp/out.perf-folded
	~/Sandbox/FlameGraph/flamegraph.pl tmp/out.perf-folded > tmp/flamegraph.svg


all: build run

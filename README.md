# Map Reduce System 


## Introduction 

I have implemented a **map reduce system** in Go. This not a production map reduce system, but targets the key aspects of map reduce implementation outlined in the seminal [Map Reduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) by Google Inc. I have developed the code for the so-called **master** and **worker** machines. Like in classic **MapReduce**, the master handles allocation of map and reduce work, and is also responsible for worker failure detection through simple mechanims of polling. The master maintains all the data structures relevant to keep track of available, ongoing, completed and delayed work. To accomplish this, it implements some form of locking scheme to ensure correctness. In the larger scheme, there is continuous ongoing communication between workers and master through remote procedure calls (RPC). I have defined RPC message, reply formats and master's server API to facilitate this communication. In this design, workers are running 2 threads in parallel, one executing map tasks and the other reduce tasks. These threads greedily request for work (using RPC) from the master. 

This project is part of the distributed projects offered through [MIT's distributed system course](https://pdos.csail.mit.edu/6.824/). To learn more about the design, look into the source code where you will find detailed comments. Below, I describe the project structure and running instructions

## Installation and Demo

1. Clone the repository
```
> git clone https://github.com/kasliwalr/map-reduce-system.git
```

2. Install Go version v1.13, use instruction provided on [Go website](https://golang.org/doc/install). I used the tarball to install Go for my machine running Ubuntu 18.04. After installation, verify you version
```
> go version
go version go1.13.15 linux/amd64
```

3. Source code structure
```
> cd map-reduce-system
> tree -L 2
.
├── LICENSE
├── main
│   ├── check.sh
│   ├── mrmaster.go
│   ├── mrsequential.go
│   ├── mr-tmp
│   ├── mrworker.go
│   ├── pg-being_ernest.txt
│   ├── pg-dorian_gray.txt
│   ├── pg-frankenstein.txt
│   ├── pg-grimm.txt
│   ├── pg-huckleberry_finn.txt
│   ├── pg-metamorphosis.txt
│   ├── pg-sherlock_holmes.txt
│   ├── pg-tom_sawyer.txt
│   ├── test-mr.sh
│   └── tmp
├── mr
│   ├── master.go
│   ├── rpc.go
│   └── worker.go
├── mrapps
│   ├── crash.go
│   ├── crash.so
│   ├── indexer.go
│   ├── indexer.so
│   ├── mtiming.go
│   ├── mtiming.so
│   ├── nocrash.go
│   ├── nocrash.so
│   ├── rtiming.go
│   ├── rtiming.so
│   ├── wc.go
│   └── wc.so
└── README.md
```

The heart of the map reduce which I implemented, is located in the `mr` directory. It contains the code for master, worker and rpc message definitions. The `mrapps` directory contains Go files implementing two applications, a word counter (`wc.go`) and an indexer (`indexer.go`). These will be build into plugins and supplied to the executable that runs the workers. Think of them as dynamically linked code. The rest of the files in `mrapps` directory are for testing purposes. All the files in `mrapps` are supplied by MIT. 
The `main` directory contains the input files, those that start with `pg` (ex. `pg-being_earnest.txt`). These represent what you would call input file chunks in **Google File System** parlance. `mrmaster.go` and `mrworker.go` are simply helper files that get the master and worker going, they simply call and pass them any input arguments. 

Although there are thorough testing scripts available in `mrapps`, I have created a simple test script, `check.sh` in main. It concatenates all output files (output of reduce function), sorts each line lexicographically and compares them against the expected output, located in `mr-test/mr-out-0`. 


3. Run the following exact commands on three separate terminals windows. Essentially, this simulates running one master and two workers concurrently on different machines
```
> cd main
> go run mrmaster.go pg-*.txt # run master on terminal 1
> go build -buildmode=plugin ../mrapps/wc.go; go run mrworker.go wc.so # run worker 1 on terminal 2
> go build -buildmode=plugin ../mrapps/wc.go; go run mrworker.go wc.so # run worker 2 on terminal 3 
```

Master and worker work silently, so you won't see much output. After running for a few seconds (10s at most), they will exit. Your `main` directory should contain a bunch of intermediate (starting with `xr`) and final output files (starting with `mr`). 

```
> ls
check.sh     mr-out-3  mr-out-8         pg-being\_ernest.txt      pg-metamorphosis.txt    wc.so    xr-1-4  xr-1-9   xr-2-4  xr-2-9   xr-3-4  xr-3-9   xr-4-4  xr-4-9   xr-5-4  xr-5-9   xr-6-4  xr-6-9   xr-7-4  xr-7-9   xr-8-4  xr-8-9
mrmaster.go  mr-out-4  mr-out-9         pg-dorian_gray.txt       pg-sherlock_holmes.txt  xr-1-1   xr-1-5  xr-2-1   xr-2-5  xr-3-1   xr-3-5  xr-4-1   xr-4-5  xr-5-1   xr-5-5  xr-6-1   xr-6-5  xr-7-1   xr-7-5  xr-8-1   xr-8-5
mr-out-1     mr-out-5  mrsequential.go  pg-frankenstein.txt      pg-tom_sawyer.txt       xr-1-10  xr-1-6  xr-2-10  xr-2-6  xr-3-10  xr-3-6  xr-4-10  xr-4-6  xr-5-10  xr-5-6  xr-6-10  xr-6-6  xr-7-10  xr-7-6  xr-8-10  xr-8-6
mr-out-10    mr-out-6  mr-tmp           pg-grimm.txt             test-mr.sh              xr-1-2   xr-1-7  xr-2-2   xr-2-7  xr-3-2   xr-3-7  xr-4-2   xr-4-7  xr-5-2   xr-5-7  xr-6-2   xr-6-7  xr-7-2   xr-7-7  xr-8-2   xr-8-7
mr-out-2     mr-out-7  mrworker.go      pg-huckleberry_finn.txt  tmp                     xr-1-3   xr-1-8  xr-2-3   xr-2-8  xr-3-3   xr-3-8  xr-4-3   xr-4-8  xr-5-3   xr-5-8  xr-6-3   xr-6-8  xr-7-3   xr-7-8  xr-8-3   xr-8-8
```

To verify that output is correct, run the test script, `check.sh`

```
> ./check.sh
ls: cannot access 'tmp': No such file or directory
22107
Files tmp and ./mr-tmp/mr-out-0 are identical 
```

## Summary

This map reduce implementation was largely undertaken to better understand what goes into building the map reduce framework. It targets the most important aspects of map reduce architecture such as dynamic load balancing, fault tolerance and ensuring correctness. There are many fine-tuning features mentioned in the MapReduce paper, which are not incorporated in this implementation, and at the time of this writing, it is working on most workloads but failing on crash tests.




package main

import (
	"adaptive/file"
	"adaptive/job"
	"adaptive/log"
	"adaptive/network"
	"adaptive/scheduler"
	"adaptive/simulator"
	"adaptive/topology"
	"adaptive/ml"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/pprof"
	"strings"
)

var logger log.Context

func check(err error) {
	if err != nil {
		logger.Fatalf("%v", err)
	}
}

func loadFiles(filename string, topo *topology.Topology, nw network.Network) (map[string]file.File, error) {
	f, err := os.Open(filename)
	check(err)
	defer f.Close()

	return file.Load(f, topo, nw)
}

func loadJobs(filename string, files map[string]file.File) ([]job.Job, error) {
	fileReader, err := os.Open(filename)
	check(err)
	defer fileReader.Close()

	return job.Load(fileReader, files)
}

func loadTopology(filename string, nw network.Network) (*topology.Topology, error) {
	reader, err := os.Open(filename)
	check(err)
	defer reader.Close()
	return topology.LoadFifo(reader, nw)
}

func printResults(results map[string]*job.Job) {

	for id, j := range results {
		tasks := make([]string, len(j.Scheduled))
		for i, task := range j.Scheduled {
			tasks[i] = fmt.Sprintf("('%s', '%s', %v, %v, %v)", j.File.Id(), task.Location, j.Submission, task.Start, task.Start+task.Duration)
		}
		fmt.Printf("%s %v [%v]\n", id, j.Submission, strings.Join(tasks, ", "))
	}
}

func printFiles(files map[string]file.File, topo *topology.Topology) {
	fmt.Print("{")
	for key, value := range files {
		locations := make([]string, 0, len(topo.DataCenters))
		for i, dc := range topo.DataCenters {
			if dc.Container().Has(key) {
				locations = append(locations, fmt.Sprintf("'DC%v'", i))
			}
		}
		fmt.Printf("'%s': (%v, [%s])", key, value.Size(), strings.Join(locations, ", "))
	}
	fmt.Println("}")
}

func main() {
	logger = log.New("main")

	schedulerPtr := flag.String("scheduler", "SRPT", "type of scheduler to be used")
	topologyPtr := flag.String("topology", "topology/full.topo", "topology description file")
	filesPtr := flag.String("files", "file/facebook.files", "files description file")
	window := flag.Uint64("window", 3, "scheduling window size")
	cpuProfilePtr := flag.String("profiler", "", "write cpu profiling to file")
	logPtr := flag.String("log", "", "file to record log")
	TrainingDataPtr := flag.String("train", "", "file to store training data")
	flag.Parse()
	
	if len(flag.Args()) < 1 {
		logger.Fatalf("missing files to run")
	}

	if *logPtr == "" {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)

	} else {
		var file *os.File
		if *logPtr == "-" {
			file = os.Stdout
		} else {
			var err error
			file, err = os.Create(*logPtr)
			if err != nil {
				logger.Fatalf("error opening log file %v: %v", *logPtr, err)
			}
		}
		log.SetLevel(log.DEBUG)
		log.EnableContext("simulator")
		log.EnableContext("topology")
		log.EnableContext("scheduler")
		log.SetOutput(file)
	}

	nw := network.NewSimpleNetwork()
	topo, err := loadTopology(*topologyPtr, &nw)
	check(err)
	files, err := loadFiles(*filesPtr, topo, &nw)
	check(err)
	printFiles(files, topo)

	filename := flag.Args()[0]
	jobs, err := loadJobs(filename, files)
	check(err)

	var sched scheduler.Scheduler
	switch *schedulerPtr {
	case "GEODIS":
		sched = scheduler.NewGeoDis(*topo)
	case "SWAG":
		sched = scheduler.NewSwag(*topo)
	case "SRPT":
		sched = scheduler.NewGRPTS(*topo)
	case "ADAPTIVE":
		sched = scheduler.NewAdaptive(*topo)
		//scheduler.Learn_data_knn()
	default:
		logger.Fatalf("unindentified scheduler %v", *schedulerPtr)
	}

	sim := simulator.New(jobs, files, topo, sched, *window)
	check(err)
	if sim == nil {
	}

	if *cpuProfilePtr != "" {
		f, err := os.Create(*cpuProfilePtr)
		if err != nil {
			logger.Fatalf("profiling error: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *TrainingDataPtr == "" {
		sim.Run(false)
	} else if *TrainingDataPtr == "LEARN" {
		ml.Learn_fn()
	} else {
		sim.Run(true)
		scheduler.Create_training_data(*TrainingDataPtr)
	}
	printResults(sched.Results())
	
}

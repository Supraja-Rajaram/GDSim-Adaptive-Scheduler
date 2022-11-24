package scheduler

/*
   This is a scheduler that will select from existing scheduler to better adapt to the properties of incoming jobs.
   This implementation will attempt to do so by estimating the makespan from each scheduler and choosing the one with the smallest makespan.
*/

import (
	/*
		"github.com/Supraja-Rajaram/gdsim_adaptive/job"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
		"github.com/Supraja-Rajaram/gdsim_adaptive/topology"
	*/

	"adaptive/ml"
	"adaptive/job"
	"adaptive/scheduler/event"
	"adaptive/topology"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/knn"
	"golang.org/x/exp/slices"
)

var knn_model *knn.KNNClassifier
var str1 string
var str2 string
var final_results [][]string

type AdaptiveScheduler struct {
	topology   topology.Topology
	jobs       []*job.Job
	schedulers []*MakespanScheduler
	results    map[string]*job.Job
	ratio      float64
}

func NewAdaptive(t topology.Topology) *AdaptiveScheduler {
	scheduler := &AdaptiveScheduler{}
	scheduler.topology = t
	scheduler.schedulers = append(scheduler.schedulers, NewSwag(t), NewGeoDis(t))
	scheduler.results = make(map[string]*job.Job)

	return scheduler
}

func (scheduler *AdaptiveScheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	scheduler.jobs = append(scheduler.jobs, j)
}

func (scheduler *AdaptiveScheduler) Results() map[string]*job.Job {
	return scheduler.results
}

func (scheduler AdaptiveScheduler) Pending() int {
	return len(scheduler.jobs)
}

// Model training for KNN
func Learn_data_knn() {

	rawData, err := base.ParseCSVToInstances("training/all_training_data.csv", true)
	if err != nil {
		panic(err)
	}
	cls := knn.NewKnnClassifier("euclidean", "linear", 2)
	cls.Fit(rawData)
	knn_model = cls
	line1 := rawData.RowString(0)
	words := strings.Fields(line1)
	str1 = words[5]
	if str1 == "SWAG" {
		str2 = "GEODIS"
	} else {
		str2 = "SWAG"
	}
}

// Predict optimal scheduler using KNN model
func Predict_knn(num_jobs int, total_tasks int, total_task_duration int, required_cpu int, available_cpu int) int {

	s1 := dataframe.NewSeriesInt64("Total_jobs", nil, num_jobs, 2)
	s2 := dataframe.NewSeriesInt64("Total_tasks", nil, total_tasks, 20)
	s3 := dataframe.NewSeriesInt64("Total_task_duration", nil, total_task_duration, 40)
	s4 := dataframe.NewSeriesInt64("Required_CPU", nil, required_cpu, 2)
	s5 := dataframe.NewSeriesInt64("Available_CPU", nil, available_cpu, 2000)
	s6 := dataframe.NewSeriesString("Scheduler", nil, str1, str2) // first sch has to be same as in training data, bug

	df := dataframe.NewDataFrame(s1, s2, s3, s4, s5, s6)

	fxgrid := base.ConvertDataFrameToInstances(df, 5)

	predictions, err := knn_model.Predict(fxgrid)

	if err != nil {
		panic(err)
	}
	scheduler_chosen := predictions.RowString(0)

	var idx int
	if scheduler_chosen == "SWAG" {
		idx = 0
	} else {
		idx = 1
	}
	return idx
}

// Predict optimal scheduler using autogluon
func Predict_autogluon(num_jobs int, total_tasks int, total_task_duration int, required_cpu int, available_cpu int) int {

	var bestIdx int
	const size = 5
	data := make([]float64, size)
	data[0] = float64(num_jobs)
	data[1] = float64(total_tasks)
	data[2] = float64(total_task_duration)
	data[3] = float64(required_cpu)
	data[4] = float64(available_cpu)
	bestIdx = ml.Predict_fn(data)

	return bestIdx
}

// Execute the batch of jobs using the specified scheduler and measure the mean job latency
// 0 - SWAG
// 1 - GEODIS
func RunScheduler(scheduler *AdaptiveScheduler, now uint64, index int) int {

	count := 0
	total_tasks := 0
	total_tasks_duration := 0
	var job_id []string
	var required int = 0

	for _, job := range scheduler.jobs {
		required += int(job.Cpus)
		job_id = append(job_id, job.Id)
		count += 1
		total_tasks += len(job.Tasks)
		total_tasks_duration += job.Total_task_duration
	}

	for _, j := range scheduler.jobs {
		scheduler.schedulers[index].Add(j)
	}

	scheduler.jobs = scheduler.jobs[:0]
	scheduler.schedulers[index].Schedule(now, true)

	for idx, sched := range scheduler.schedulers {

		jobs := sched.heap.Flush()
		if idx == index {
			for _, job := range jobs {
				scheduler.jobs = append(scheduler.jobs, &job.Job)
			}
		}
	}

	total_latency := 0
	for id, j := range scheduler.schedulers[index].Results() {
		scheduler.results[id] = j

		if slices.Contains(job_id, id) {
			job_end := j.Submission
			for _, task := range j.Scheduled {
				task_end := task.Start + task.Duration
				if task_end > job_end {
					job_end = task_end
				}
			}
			total_latency += int(job_end - j.Submission)
		}
	}

	return total_latency
}

func (scheduler *AdaptiveScheduler) Schedule(now uint64, training_enabled bool) []event.Event {

	if training_enabled {
		return scheduler.Schedule_Train(now)
	} else {
		return scheduler.Schedule_Adaptive(now)
	}
}

// Schedule jobs using Adaptive scheduler
func (scheduler *AdaptiveScheduler) Schedule_Adaptive(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)

	job_count := 0
	total_tasks := 0
	total_tasks_duration := 0

	var required_cpu int = 0
	for _, job := range scheduler.jobs {
		required_cpu += int(job.Cpus)
		job_count += 1
		total_tasks += len(job.Tasks)
		total_tasks_duration += job.Total_task_duration
	}
	total := 0
	available_cpu := 0
	for _, dc := range scheduler.topology.DataCenters {
		total += dc.JobCapacity(1)
		available_cpu += dc.JobAvailability(1)
	}
	bestIdx := 0
	if job_count != 0 {
		//bestIdx = Predict_knn(job_count,total_tasks,total_tasks_duration,required_cpu,available_cpu)
		bestIdx = Predict_autogluon(job_count, total_tasks, total_tasks_duration, required_cpu, available_cpu)
	}
	for _, j := range scheduler.jobs {
		scheduler.schedulers[bestIdx].Add(j)
	}

	scheduler.jobs = scheduler.jobs[:0]
	events := scheduler.schedulers[bestIdx].Schedule(now, false)

	for idx, sched := range scheduler.schedulers {
		jobs := sched.heap.Flush()
		if idx == bestIdx {
			for _, job := range jobs {
				scheduler.jobs = append(scheduler.jobs, &job.Job)
			}
		}
	}
	for id, j := range scheduler.schedulers[bestIdx].Results() {
		scheduler.results[id] = j
	}

	return events
}

// Schedule jobs using both GEODIS and SWAG during training
func (scheduler *AdaptiveScheduler) Schedule_Train(now uint64) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)

	geo_topo := topology.CopyTopo(scheduler.topology)
	geo_scheduler := NewAdaptive(*geo_topo)
	swag_topo := topology.CopyTopo(scheduler.topology)
	swag_scheduler := NewAdaptive(*swag_topo)

	var bestIdx int
	job_count := 0
	total_tasks := 0
	total_tasks_duration := 0
	var job_id []string
	var required_cpu int = 0
	for _, job := range scheduler.jobs {
		required_cpu += int(job.Cpus)
		job_id = append(job_id, job.Id)
		job_count += 1
		total_tasks += len(job.Tasks)
		total_tasks_duration += job.Total_task_duration
	}

	// Find current available CPU count
	total := 0
	available_cpu := 0
	for _, dc := range scheduler.topology.DataCenters {
		total += dc.JobCapacity(1)
		available_cpu += dc.JobAvailability(1)
	}

	for _, j := range scheduler.jobs {
		swag_scheduler.Add(j)
		geo_scheduler.Add(j)
	}

	swag_latency := RunScheduler(swag_scheduler, now, 0)  // SWAG
	geodis_latency := RunScheduler(geo_scheduler, now, 1) // GEODIS

	chosen := "GEODIS"

	if swag_latency < geodis_latency {
		chosen = "SWAG"
		bestIdx = 0
	} else {
		bestIdx = 1
	}

	for _, j := range scheduler.jobs {
		scheduler.schedulers[bestIdx].Add(j)
	}

	scheduler.jobs = scheduler.jobs[:0]
	events := scheduler.schedulers[bestIdx].Schedule(now, true)

	for idx, sched := range scheduler.schedulers {

		jobs := sched.heap.Flush()
		if idx == bestIdx {
			for _, job := range jobs {
				scheduler.jobs = append(scheduler.jobs, &job.Job)
			}
		}
	}

	if job_count == 0 {
		return events
	}

	// Store the training data in final_results
	job_data := []string{strconv.Itoa(job_count), strconv.Itoa(total_tasks), strconv.Itoa(total_tasks_duration), strconv.Itoa(required_cpu), strconv.Itoa(available_cpu), chosen}
	final_results = append(final_results, job_data)
	// fmt.Println(final_results)
	return events
}

// Upload training data to a csv file
func Create_training_data(filename string) {

	f, err := os.Create(filename)
	defer f.Close()

	if err != nil {
		fmt.Println("failed to open file", err)
	}

	w := csv.NewWriter(f)
	defer w.Flush()

	for _, job_data := range final_results {
		err = w.Write(job_data)
		if err != nil {
			fmt.Println("error writing record to file", err)
		}
	}
}

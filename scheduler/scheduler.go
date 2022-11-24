package scheduler

import (
	"fmt"
	"sort"

	/*
		"github.com/Supraja-Rajaram/gdsim_adaptive/file"
		"github.com/Supraja-Rajaram/gdsim_adaptive/job"
		"github.com/Supraja-Rajaram/gdsim_adaptive/log"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
		"github.com/Supraja-Rajaram/gdsim_adaptive/topology"
	*/

	"adaptive/file"
	"adaptive/job"
	"adaptive/log"
	"adaptive/scheduler/event"
	"adaptive/topology"
)

var logger log.Context

func init() {
	logger = log.New("scheduler")
}

type jobHeap []*job.Job

// TODO: I shouldn't be calling rpt all the time
// Make it so the first time calculates, then it marks the job as clean/dirty
func rpt(j job.Job) uint64 {
	var total uint64
	for _, task := range j.Tasks {
		total += task.Duration
	}
	return total
}

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return rpt(*h[i]) < rpt(*h[j]) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*job.Job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h jobHeap) Top() *job.Job {
	return h[0]
}

func transferTime(size uint64, t topology.Topology, from, to int) uint64 {
	if from == to {
		return 0
	}
	return size / t.Speeds[from][to]
}

type transferCenter struct {
	transferTime           uint64
	freeJobSlots, capacity int
	dataCenter             topology.DataCenter
}

/*
Returns a list of data centers suitable for running a job that requires file f,
sorted by transfer time in topology t and with capacity according to cost.
*/
func fullBestDcs(f file.File, t topology.Topology, cost int) []transferCenter {
	res := make([]transferCenter, len(t.DataCenters))
	locations := make([]int, 0, len(t.DataCenters))
	for i, dc := range t.DataCenters {
		if dc.Container().Has(f.Id()) {
			locations = append(locations, i)
		}
	}

	for i := range t.DataCenters {
		res[i].dataCenter = t.DataCenters[i]
		res[i].transferTime = transferTime(f.Size(), t, locations[0], i)
		res[i].capacity = t.DataCenters[i].JobCapacity(cost)
		res[i].freeJobSlots = t.DataCenters[i].JobAvailability(cost)
		for k := 1; k < len(locations); k++ {
			from := locations[k]
			if transfer := transferTime(f.Size(), t, from, i); transfer < res[i].transferTime {
				res[i].transferTime = transfer
			}
		}
	}
	sort.Slice(res, func(i, k int) bool { return res[i].transferTime < res[k].transferTime })
	return res
}

type transferFileEvent struct {
	f     file.File
	where topology.DataCenter
	when  uint64
}

func (event transferFileEvent) Time() uint64 {
	return event.when
}

func (tfe transferFileEvent) Process() []event.Event {
	return tfe.where.Container().Transfer(tfe.when, tfe.f.Id(), tfe.f,
		func(time uint64) []event.Event { return nil })
}

type taskEndEvent struct {
	start, duration uint64
	cpus            int
	where           int
	job             *job.Job
	transferTime    uint64
}

func (event taskEndEvent) End() uint64 {
	return event.start + event.duration
}

func (event taskEndEvent) Cpus() int {
	return event.cpus
}

func (event *taskEndEvent) SetStart(start uint64) {
	event.start = start + event.transferTime
}

func (event *taskEndEvent) SetWhere(where int) {
	event.where = where
}

func (event taskEndEvent) Process() []event.Event {
	logger.Debugf("%v.Process()", event)
	event.job.Scheduled = append(event.job.Scheduled, job.DoneTask{
		Start:    event.start,
		Duration: event.duration,
		Location: fmt.Sprintf("DC%v", event.where),
	})
	logger.Infof("added event to Scheduled - len(Scheduled) = %v\n", len(event.job.Scheduled))
	return nil
}

type Scheduler interface {
	//Pop() *job.Task
	Add(t *job.Job)
	Schedule(now uint64, training_enabled bool) []event.Event
	Results() map[string]*job.Job
	Pending() int
}

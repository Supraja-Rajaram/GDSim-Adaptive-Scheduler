package scheduler

import (
	"container/heap"
	"sort"

	/*
		"github.com/Supraja-Rajaram/gdsim_adaptive/job"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
		"github.com/Supraja-Rajaram/gdsim_adaptive/topology"
	*/

	"adaptive/job"
	"adaptive/scheduler/event"
	"adaptive/topology"
)

type GlobalSRPTScheduler struct {
	heap     jobHeap
	topology topology.Topology
	jobs     map[string]*job.Job
}

func NewGRPTS(t topology.Topology) *GlobalSRPTScheduler {
	scheduler := &GlobalSRPTScheduler{
		topology: t,
		jobs:     make(map[string]*job.Job),
	}
	heap.Init(&scheduler.heap)
	return scheduler
}

func (scheduler *GlobalSRPTScheduler) Add(j *job.Job) {
	logger.Debugf("%p.Add(%p)", scheduler, j)
	sort.Slice(j.Tasks, func(i, k int) bool { return j.Tasks[i].Duration < j.Tasks[k].Duration })
	heap.Push(&scheduler.heap, j)
	scheduler.jobs[j.Id] = j
}

func (scheduler GlobalSRPTScheduler) Pending() int {
	return scheduler.heap.Len()
}

func (scheduler *GlobalSRPTScheduler) Schedule(now uint64, training_enabled bool) []event.Event {
	logger.Debugf("%p.Schedule(%v)", scheduler, now)
	events := make([]event.Event, 0)
	for scheduler.heap.Len() > 0 {
		top := scheduler.heap[0]
		dcs := fullBestDcs(top.File, scheduler.topology, int(top.Cpus))
		for len(top.Tasks) > 0 {
			hosted := false
			for _, dc := range dcs {
				task := top.Tasks[len(top.Tasks)-1]
				taskEnd := &taskEndEvent{
					start:    dc.transferTime + now,
					duration: task.Duration,
					cpus:     int(top.Cpus),
					job:      top,
				}
				if node, success := dc.dataCenter.Host(taskEnd); success {
					top.Tasks = top.Tasks[:len(top.Tasks)-1]
					if node != nil {
						taskEnd.where = node.Location
						if node.QueueLen() == 1 {
							events = append(events, node)
						}
					}
					hosted = true
					logger.Infof("scheduling task %v for job %v", task, top.Id)
					break
				} else {
					logger.Infof("failed scheduling task for job %p", top.Id)
				}
			}
			if !hosted {
				return events
			}
		}
		heap.Pop(&scheduler.heap)
	}
	return events
}

func (scheduler GlobalSRPTScheduler) Results() map[string]*job.Job {
	return scheduler.jobs
}

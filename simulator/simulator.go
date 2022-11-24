package simulator

import (
	"container/heap"

	/*
		"github.com/Supraja-Rajaram/gdsim_adaptive/file"
		"github.com/Supraja-Rajaram/gdsim_adaptive/job"
		"github.com/Supraja-Rajaram/gdsim_adaptive/log"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
		"github.com/Supraja-Rajaram/gdsim_adaptive/topology"
	*/

	"adaptive/file"
	"adaptive/job"
	"adaptive/log"
	"adaptive/scheduler"
	"adaptive/scheduler/event"
	"adaptive/topology"
)

var logger log.Context
var train bool

func init() {
	logger = log.New("simulator")
}

type JobArrival struct {
	Job       job.Job
	Scheduler scheduler.Scheduler
}

func (arrival JobArrival) Time() uint64 {
	return arrival.Job.Submission
}

func (arrival JobArrival) Process() []event.Event {
	arrival.Scheduler.Add(&arrival.Job)
	return nil
}

type WindowScheduling struct {
	When      uint64
	Window    uint64
	Scheduler scheduler.Scheduler
	sim       *Simulation
}

func (scheduling WindowScheduling) Time() uint64 {
	return scheduling.When
}

func (scheduling WindowScheduling) Process() []event.Event {
	logger.Debugf("window(%d) Process()", scheduling.When)
	logger.Debugf("%d tasks remaining", scheduling.sim.Len())
	logger.Debugf("%d jobs remaining", scheduling.Scheduler.Pending())
        
        jobEvents := scheduling.Scheduler.Schedule(scheduling.When, train)
	if scheduling.sim.Len() > 0 || scheduling.Scheduler.Pending() > 0 {
		when := scheduling.When + scheduling.Window
		logger.Debugf("first when: %d (%d + %d)", when, scheduling.When, scheduling.Window)
		if scheduling.sim.Len() > 0 {
			if nextEvent := scheduling.sim.Next(); when <= nextEvent {
				logger.Debugf("next event: %d", nextEvent)
				when = nextEvent - nextEvent%scheduling.Window + scheduling.Window
			}
		}
		next := WindowScheduling{
			When:      when,
			Window:    scheduling.Window,
			Scheduler: scheduling.Scheduler,
			sim:       scheduling.sim,
		}
		jobEvents = append(jobEvents, next)
	}
	logger.Debugf("returning %d events", len(jobEvents))
	return jobEvents
}

type Simulation struct {
	Jobs      []job.Job
	Files     map[string]file.File
	Topo      *topology.Topology
	Heap      event.EventHeap
	Scheduler scheduler.Scheduler
}

func New(jobs []job.Job, files map[string]file.File, topo *topology.Topology, scheduler scheduler.Scheduler, window uint64) *Simulation {
	sim := &Simulation{
		Jobs:      jobs,
		Files:     files,
		Topo:      topo,
		Scheduler: scheduler,
	}
	heap.Init(&sim.Heap)
	min := jobs[0].Submission
	for _, j := range jobs {
		heap.Push(&sim.Heap, JobArrival{
			Job:       j,
			Scheduler: scheduler,
		})
		if j.Submission < min {
			min = j.Submission
		}
	}
	heap.Push(&sim.Heap, WindowScheduling{
		When:      min + 1,
		Window:    window,
		Scheduler: scheduler,
		sim:       sim,
	})

	return sim
}

func (simulation *Simulation) Run(training_enabled bool) ([]Result, error) {
	// Create JobArrival Events
	// While there are events to process
	// Process next event
	logger.Debugf("Run()")
	train = training_enabled
	for len(simulation.Heap) > 0 {
		e := heap.Pop(&simulation.Heap).(event.Event)
		logger.Infof("simulator popped event of type %T", e)
		logger.Debugf("heap at location %p", &simulation.Heap)
		if simulation.Heap.Len() > 0 {
			logger.Infof("next event is of type %T", simulation.Heap[0])
		}
		logger.Infof("%d events remaining:", len(simulation.Heap))
		for _, new_event := range e.Process() {
			logger.Infof("simulator adding event of type %T", new_event)
			heap.Push(&simulation.Heap, new_event)
		}
	}
	return nil, nil
}

func (simulation Simulation) Len() int {
	return simulation.Heap.Len()
}

func (simulation Simulation) Next() uint64 {
	return simulation.Heap[0].Time()
}

type Result struct {
	Job       *job.Job
	Durations []uint64
}

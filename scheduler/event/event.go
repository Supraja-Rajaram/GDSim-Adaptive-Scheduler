package event

import (
	"container/heap"

	//"github.com/Supraja-Rajaram/gdsim_adaptive/log"

	"adaptive/log"
)

var logger log.Context

func init() {
	logger = log.New("event")
}

type Event interface {
	Time() uint64
	Process() []Event
}

type EventHeap []Event

func NewEventHeap() EventHeap          { return make([]Event, 0) }
func (h EventHeap) Len() int           { return len(h) }
func (h EventHeap) Less(i, j int) bool { return h[i].Time() < h[j].Time() }
func (h EventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *EventHeap) Push(x interface{}) {
	*h = append(*h, x.(Event))
	logger.Infof("added %p", &x)
}

func (h *EventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h EventHeap) Top() Event {
	return h[0]
}

func (h *EventHeap) Process() {
	logger.Infof("%p.Process()", h)
	event := heap.Pop(h).(Event)
	if consequences := event.Process(); len(consequences) > 0 {
		for _, e := range consequences {
			heap.Push(h, e)
		}
	}
}

func Simulate(seed []Event) {
	var evHeap EventHeap
	heap.Init(&evHeap)
	for _, event := range seed {
		heap.Push(&evHeap, event)
	}

	for len(evHeap) > 0 {
		evHeap.Process()
	}
}

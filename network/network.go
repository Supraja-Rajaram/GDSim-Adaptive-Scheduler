package network

import (
	"container/heap"
	"fmt"

	//"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"

	"adaptive/scheduler/event"
)

type TransferEvent struct {
	consequence func(time uint64) []event.Event
	when        uint64
}

func (te TransferEvent) Time() uint64 {
	return te.when
}

func (te TransferEvent) Process() []event.Event {
	return te.consequence(te.Time())
}

// Network is meant to represent transfer of data between data centers.
type Network interface {

	// Transfer models a transfer starting at time now of size
	// bytes from one location to another with a third parameter
	// to define what happens as a consequence of this transfer.
	// It returns a TransferEvent, which will execute the
	// consequence function at the time the transfer ends, and a
	// possible error if there's a problem with the transfer.
	StartTransfer(when, size uint64, from, to string, consequence func(time uint64) []event.Event) ([]event.Event, error)

	// Advances the network simulation up to time.  Returns the
	// earliest network events that concluded before that time,
	// the time they happened, or an error.
	Advance(time uint64) ([]TransferEvent, uint64, error)

	// Status returns a LinkStatus struct describing the current
	// condition of the link identified by the from, to ids
	Status(from, to string) (LinkStatus, error)

	AddConnection(from, to string, speed, delay uint64)
}

type LinkStatus struct {
	// I have to think about what to put here

	// available bandwidth
	Bandwidth uint64
}

type connection struct {
	speed, delay uint64
	status       LinkStatus
}

// SimpleNetwork models a naive approach to simulating a network.
type SimpleNetwork struct {
	heap        event.EventHeap
	connections map[string]map[string]connection
}

func NewSimpleNetwork() SimpleNetwork {
	return SimpleNetwork{
		heap:        event.NewEventHeap(),
		connections: make(map[string]map[string]connection),
	}
}

func (network SimpleNetwork) AddConnection(from, to string, speed, delay uint64) {
	f, ok := network.connections[from]
	if !ok {
		network.connections[from] = make(map[string]connection)
		f = network.connections[from]
	}
	f[to] = connection{
		speed: speed,
		delay: delay,
	}
}

func (network *SimpleNetwork) StartTransfer(when, size uint64, from, to string, consequence func(time uint64) []event.Event) ([]event.Event, error) {
	if network.connections == nil {
		return nil, fmt.Errorf("no topology defined for SimpleNetwork")
	}
	f, ok := network.connections[from]
	if !ok {
		return nil, fmt.Errorf("from id %v not in topology", from)
	}
	conn, ok := f[to]
	if !ok {
		return nil, fmt.Errorf("to id %v not in topology", to)
	}
	time := when + conn.delay + size/conn.speed
	heap.Push(&network.heap, TransferEvent{
		when:        time,
		consequence: consequence,
	})
	return nil, nil
}

func (network *SimpleNetwork) Advance(time uint64) ([]TransferEvent, uint64, error) {
	if len(network.heap) == 0 {
		return nil, time, nil
	}
	first := network.heap.Top().Time()
	if first > time {
		return nil, time, nil
	}
	events := make([]TransferEvent, 0)
	for network.heap.Top().Time() == first {
		events = append(events, heap.Pop(&network.heap).(TransferEvent))
	}
	return events, first, nil
}

func (network *SimpleNetwork) Status(from, to string) (LinkStatus, error) {
	if f, ok := network.connections[from]; ok {
		if _, ok := f[to]; !ok {
			return LinkStatus{}, fmt.Errorf("no link from %v to %v", from, to)
		}
	} else {
		return LinkStatus{}, fmt.Errorf("no link from %v to %v", from, to)
	}
	return network.connections[from][to].status, nil
}

package topology

import (
	"container/heap"
	"fmt"
	"io"

	/*
		"github.com/Supraja-Rajaram/gdsim_adaptive/log"
		"github.com/Supraja-Rajaram/gdsim_adaptive/network"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
	*/
	"github.com/google/go-cmp/cmp"

	"adaptive/log"
	"adaptive/network"
	"adaptive/scheduler/event"
)

var logger log.Context

func init() {
	logger = log.New("topology")
}

type RunningTask interface {
	End() uint64
	Cpus() int
	SetStart(start uint64)
	SetWhere(where int)
	Process() []event.Event
}

type Data interface {
	Id() string
	Size() uint64
}

// Database is an interface to model information about file storage
type Database interface {
	// Location should return a list of location ids for a file with given id
	Location(fileId string) []string

	// Record will update the database to store that the file with given id can be found at the given location
	Record(fileId, locationId string)
}

// Container is an interface to model storage for data
type Container interface {
	Add(id string, data Data)
	Has(id string) bool
	Find(id string) Data
	Pop(id string) Data
	Transfer(when uint64, id string, data Data, consequence func(time uint64) []event.Event) []event.Event
	SetNetwork(network network.Network)
	SetDatabase(db Database)
}

type DataCenter interface {
	Enqueue(rt RunningTask)
	Dequeue(now uint64, calling *Node) []event.Event
	JobCapacity(cost int) int
	JobAvailability(cost int) int
	ExpectedEndings() []uint64
	Host(task RunningTask) (*Node, bool)
	Equal(otherDc DataCenter) bool
	Container() Container
	AddContainer(container Container)
	NumNodes() int
	Nodes() []*Node
	Id() string

	// this function meant for testing
	Get(n int) *Node
}

type taskHeap []RunningTask

func NewTaskHeap() taskHeap {
	return make([]RunningTask, 0)
}

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].End() < h[j].End() }
func (h taskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x interface{}) {
	*h = append(*h, x.(RunningTask))
}

func (h taskHeap) Top() RunningTask {
	return h[0]
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Node struct {
	Location   int
	freeCpus   int
	capacity   int
	heap       taskHeap
	datacenter DataCenter
}

type FifoDataCenter struct {
	id        int
	nodes     []*Node
	container Container
	nodeMax   int /* maximum capacity of a single node */
	queue     taskHeap
	/* tasks that have been assigned to this data center but
	   cannot be scheduled yet */
}

func (dc FifoDataCenter) Id() string {
	return fmt.Sprintf("DC%d", dc.id)
}

func (dc FifoDataCenter) Container() Container {
	return dc.container
}

func (dc *FifoDataCenter) AddContainer(container Container) {
	dc.container = container
}

func (dc *FifoDataCenter) NumNodes() int {
	return len(dc.nodes)
}

func (dc *FifoDataCenter) Nodes() []*Node {
	return dc.nodes
}

/*
Returns how many jobs requiring *cost* CPU slots a data center can host at most.
*/
func (dc FifoDataCenter) JobCapacity(cost int) int {
	return (dc.nodes[0].capacity / cost) * len(dc.nodes)
}

/*
Returns how many jobs requiring *cost* CPU slots a data center can currently host
given available free space.
*/
func (dc FifoDataCenter) JobAvailability(cost int) (free int) {
	for _, n := range dc.nodes {
		free += n.freeCpus / cost
	}
	return free
}

/*
Returns a pointer to the corresponding node.
This is meant only to be used in tests.
*/
func (dc FifoDataCenter) Get(n int) *Node {
	if n < len(dc.nodes) {
		return dc.nodes[n]
	}
	return nil
}

/*
   Returns the expected ending times for all tasks currently hosted in dc.
*/
func (dc FifoDataCenter) ExpectedEndings() []uint64 {
	endings := make([]uint64, 0)
	for _, node := range dc.nodes {
		for _, task := range node.heap {
			endings = append(endings, task.End())
		}
	}
	return endings
}

/*
   Compares two DataCenters. They are equal if they have the same
   amount of nodes and all nodes have the same free capacity.
*/
func (dc FifoDataCenter) Equal(otherDc DataCenter) bool {
	other, ok := otherDc.(*FifoDataCenter)
	if !ok {
		return false
	}
	if len(dc.nodes) != len(other.nodes) {
		return false
	}
	// TODO: this assumes that the order of the nodes was not changed
	// it will possible require a fix
	for i := range dc.nodes {
		if dc.nodes[i].freeCpus != other.nodes[i].freeCpus {
			return false
		}
	}
	return true
}

func (dc *FifoDataCenter) Enqueue(rt RunningTask) {
	heap.Push(&dc.queue, rt)
}

func (dc *FifoDataCenter) Dequeue(now uint64, calling *Node) []event.Event {
	events := make([]event.Event, 0)
	for dc.queue.Len() > 0 {
		task := dc.queue.Top()
		task.SetStart(now)
		success := false
		for _, n := range dc.nodes {
			if n.Host(task) {
				heap.Pop(&dc.queue)
				success = true
				if n != calling && n.QueueLen() == 1 {
					events = append(events, n)
				}
				break
			}
		}
		if !success {
			break
		}
	}
	return events
}

type Topology struct {
	DataCenters []DataCenter
	Speeds      [][]uint64
}

var topo_capacity [][2]int
var topo_speed [][]uint64
var topo_nw network.Network

// NewFifo creates a new topology using FIFO scheduling in all data centers.
// capacity holds the number of computers and number of cores in each computer
// speed holds the bandwidth between datacenters
// nw is the network that will be used to connect the data centers
func NewFifo(capacity [][2]int, speeds [][]uint64, nw network.Network) (*Topology, error) {
	var topo Topology
	topo.DataCenters = make([]DataCenter, len(capacity))
	topo.Speeds = make([][]uint64, len(capacity))
	topo_capacity = capacity
	topo_speed = speeds
	topo_nw = nw
	if len(speeds) != len(capacity) {
		return nil, fmt.Errorf("len(capacity)=%d != len(speeds)=%d", len(capacity), len(speeds))
	}
	for i, dc := range capacity {
		nNodes := dc[0]
		nCpus := dc[1]
		n := make([]*Node, nNodes)
		dc := &FifoDataCenter{
			id:      i,
			nodes:   n,
			nodeMax: nCpus,
		}
		for k := range dc.nodes {
			dc.nodes[k] = NewNode(nCpus, i)
			dc.nodes[k].datacenter = dc
		}
		topo.DataCenters[i] = dc
		if len(speeds[i]) != len(capacity) {
			return nil, fmt.Errorf("len(capacity)=%d != len(speeds[%d])=%d", len(capacity), i, len(speeds))
		}
	}
	for i := range capacity {
		topo.Speeds[i] = make([]uint64, len(capacity))
		for k := range speeds[i] {
			topo.Speeds[i][k] = speeds[i][k]
			// TODO: the delay is hardcoded at 10ms until I find better values
			nw.AddConnection(topo.DataCenters[i].Id(), topo.DataCenters[k].Id(), speeds[i][k], 10)
		}
	}
	return &topo, nil
}

func LoadFifo(topoInfo io.Reader, nw network.Network) (*Topology, error) {
	var size int

	n, err := fmt.Fscan(topoInfo, &size)
	if err != nil {
		return nil, fmt.Errorf("failure to read topology: size error: %v", err)
	} else if n != 1 {
		return nil, fmt.Errorf("failure to read topology: size error: missing size")
	}

	capacity := make([][2]int, size)
	for i := 0; i < size; i++ {
		n, err := fmt.Fscan(topoInfo, &capacity[i][0], &capacity[i][1])
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: data center %v: %v", i, err)
		} else if n != 2 {
			return nil, fmt.Errorf("failure to read topology: data center %v: missing elements in capacity line", i)
		}
	}
	speeds := make([][]uint64, size)
	for i := 0; i < size; i++ {
		speeds[i] = make([]uint64, size)
		/*_, err := fmt.Fscanf(topoInfo, "\n")
		if err != nil {
			return nil, fmt.Errorf("failure to read topology: speeds %v: %v", i, err)
		}*/
		for k := 0; k < size; k++ {
			n, err := fmt.Fscan(topoInfo, &speeds[i][k])
			if n != 1 {
				return nil, fmt.Errorf("failure to read topology: speeds %v: missing speeds", i)
			} else if err != nil {
				return nil, fmt.Errorf("failure to read topology: speeds %v: %v", i, err)
			}
		}
	}
	// TODO: inspect here for proper validation of speeds

	return NewFifo(capacity, speeds, nw)
}

func NewNode(capacity int, location int) *Node {
	var n Node
	n.freeCpus = capacity
	n.capacity = capacity
	n.Location = location
	n.heap = NewTaskHeap()
	heap.Init(&n.heap)
	return &n
}

func (n *Node) Host(task RunningTask) bool {
	if task.Cpus() <= n.freeCpus {
		task.SetWhere(n.Location)
		task.Process()
		n.freeCpus -= task.Cpus()
		heap.Push(&n.heap, task)
		return true
	} // TODO: add else case to allow running with less CPUs than requested
	logger.Debugf("node failed to host task with %d CPUS: available capacity is %d", task.Cpus(), n.freeCpus)
	return false
}

func (n *Node) Free(cpus int) {
	n.freeCpus += cpus
}

func (n *Node) QueueLen() int {
	return n.heap.Len()
}

func (n *Node) Process() []event.Event {
	logger.Debugf("%p.Process()", n)
	now := n.Time()
	t := heap.Pop(&n.heap).(RunningTask)
	n.Free(t.Cpus())
	var events []event.Event
	if n.datacenter != nil {
		events = n.datacenter.Dequeue(now, n)
	} else {
		events = make([]event.Event, 0)
	}
	if n.heap.Len() > 0 {
		logger.Infof("keeping node %p: %d tasks remaining", n, n.heap.Len())
		return append(events, n)
	}
	logger.Infof("removing node %p", n)
	return events
}

func (n *Node) Time() uint64 {
	logger.Debugf("%p.Time()", n)
	if len(n.heap) == 0 {
		logger.Fatalf("node %p has no tasks", n)
		return 0
	}
	return n.heap[0].End()
}

func (dc *FifoDataCenter) Host(task RunningTask) (*Node, bool) {
	logger.Debugf("%p.Host()", dc)
	if task.Cpus() > dc.nodeMax {
		return nil, false
	}
	for _, n := range dc.nodes {
		if n.Host(task) {
			return n, true
		}
	}
	dc.Enqueue(task)
	return nil, true
}

func (topo Topology) Equal(other Topology) bool {
	if len(topo.DataCenters) != len(other.DataCenters) {
		return false
	}
	for i := range topo.DataCenters {
		if !topo.DataCenters[i].Equal(other.DataCenters[i]) {
			return false
		}
	}
	return cmp.Equal(topo.Speeds, other.Speeds)
}

// adaptive code
func CopyTopo(topology Topology) *Topology {
	var topo Topology
	topo.DataCenters = make([]DataCenter, len(topo_capacity))
	topo.Speeds = make([][]uint64, len(topology.Speeds))
	copy(topo.Speeds, topology.Speeds)

	for i, dc := range topo_capacity {
		nNodes := dc[0]
		nCpus := dc[1]
		n := make([]*Node, nNodes)
		dc := &FifoDataCenter{
			id:      i,
			nodes:   n,
			nodeMax: nCpus,
		}
		for k := range dc.nodes {
			dc.nodes[k] = NewNode(nCpus, i)
			dc.nodes[k].capacity = topology.DataCenters[i].Nodes()[k].capacity
			dc.nodes[k].Location = topology.DataCenters[i].Nodes()[k].Location
			dc.nodes[k].freeCpus = topology.DataCenters[i].Nodes()[k].freeCpus
			dc.nodes[k].heap = topology.DataCenters[i].Nodes()[k].heap
			dc.nodes[k].datacenter = dc

		}
		topo.DataCenters[i] = dc
	}

	for i, dc := range topology.DataCenters {
		topo.DataCenters[i].AddContainer(dc.Container())
	}

	return &topo
}

package topology

import (
	"strings"
	"testing"

	"container/heap"

	"adaptive/scheduler/event"
	//"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
	"github.com/google/go-cmp/cmp"
)

// dummy struct to be used in tests
type sampleTask struct {
	end  uint64
	cpus int
}

func (t sampleTask) End() uint64            { return t.end }
func (t sampleTask) Cpus() int              { return t.cpus }
func (t sampleTask) SetStart(start uint64)  {}
func (t sampleTask) SetWhere(where int)     {}
func (t sampleTask) Process() []event.Event { return nil }

func checkHeap(t *testing.T, h taskHeap, length int, top uint64) {
	if l := h.Len(); l != length {
		t.Fatalf("expected heap.Len() == %d, found %d", length, l)
	} else if l > 0 {
		if f := h.Top().End(); f != top {
			t.Fatalf("expected heap.Top() == %d, found %d", top, f)
		}
	}
}

func TestTaskHeap(t *testing.T) {
	h := NewTaskHeap()
	tasks := []sampleTask{
		{2, 1},
		{3, 1},
		{0, 1},
		{1, 1},
	}

	checkHeap(t, h, 0, 0)
	v := tasks[0].End()
	for i, task := range tasks {
		heap.Push(&h, task)
		if task.End() < v {
			v = task.End()
		}
		checkHeap(t, h, i+1, v)
	}

	for cur := h.Top(); h.Len() > 0; {
		p := heap.Pop(&h).(RunningTask)
		if p.End() < cur.End() {
			t.Fatalf("expected Pop(heap) >= %d, found %d", cur.End(), p.End())
		}
		cur = p
	}
}

func TestDataCenterEqual(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{1, 2},
		{2, 1},
		{2, 1},
	}
	speed := [][]uint64{
		{0, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 0},
	}
	topo, err := NewFifo(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	if topo.DataCenters[0].Equal(topo.DataCenters[2]) {
		t.Errorf("expected %v.Equal(%v) == false, found true", topo.DataCenters[0], topo.DataCenters[2])
	}
	if !topo.DataCenters[0].Equal(topo.DataCenters[1]) {
		t.Errorf("expected %v.Equal(%v) == true, found false", topo.DataCenters[0], topo.DataCenters[1])
	}
	if !topo.DataCenters[2].Equal(topo.DataCenters[3]) {
		t.Errorf("expected %v.Equal(%v) == true, found false", topo.DataCenters[2], topo.DataCenters[3])
	}
}

func TestNewFifo(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
		{3, 5},
		{1, 3},
	}
	speed := [][]uint64{
		{0, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 0},
	}

	topo, err := NewFifo(cap, speed)
	if err != nil {
		t.Errorf("expected err = nil, found %v", err)
	}
	if len(topo.DataCenters) != len(cap) {
		t.Errorf("expected len(topo.DataCenters) == %d, found %d", len(cap), len(topo.DataCenters))
	}

	for i, dc := range topo.DataCenters {
		if cap[i][0] != dc.NumNodes() {
			t.Errorf("expected len(DataCenter[%d]) = %d, found %d", i, cap[i][0], dc.NumNodes())
		}
		for k, n := range dc.Nodes() {
			if cap[i][1] != n.freeCpus {
				t.Errorf("expected node[%d].freeCpus = %d, found %d", k, cap[i][1], n.freeCpus)
			}
		}
	}
	if !cmp.Equal(speed, topo.Speeds) {
		t.Errorf("expected topo.Speeds = %v, found %v", speed, topo.Speeds)
	}

	badSpeed := [][]uint64{
		{0, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 0},
		{1, 1, 1, 0},
	}
	_, err = NewFifo(cap, badSpeed)
	if err == nil {
		t.Errorf("expected err != nil, found nil")
	}
	badSpeed = [][]uint64{
		{0, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 0, 1, 0},
		{1, 1, 1, 0},
	}
	_, err = NewFifo(cap, badSpeed)
	if err == nil {
		t.Errorf("expected err != nil, found nil")
	}
}

func TestDCCapacity(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
		{3, 5},
		{1, 3},
	}
	speed := [][]uint64{
		{0, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 0},
	}

	topo, err := NewFifo(cap, speed)
	if err != nil {
		t.Fatalf("failed to build topology: %v", err)
	}
	keys := map[int][]int{
		1: {2, 2, 15, 3},
		2: {1, 0, 6, 1},
	}
	for k, key := range keys {
		for i, dc := range topo.DataCenters {
			if cap := dc.JobCapacity(k); cap != key[i] {
				t.Errorf("wrong JobCapacity(%d) for dc[%d]: expected %d, found %d", k, i, key[i], cap)
			}
		}
	}
}

func TestNodeHost(t *testing.T) {
	t1 := sampleTask{
		end:  10,
		cpus: 5,
	}
	t2 := sampleTask{
		end:  20,
		cpus: 2,
	}

	n := NewNode(4, 0)
	if n.Host(t1) {
		t.Errorf("expected n.Host(5) = fail, found success")
	}
	if n.freeCpus != 4 {
		t.Errorf("expected n.freeCpus = 4, found %d", n.freeCpus)
	}
	if n.heap.Len() != 0 {
		t.Errorf("expected n.heap.Len() = 0, found %d", n.heap.Len())
	}

	if !n.Host(t2) {
		t.Errorf("expected n.Host(2) = true, found false")
	}
	if n.freeCpus != 2 {
		t.Errorf("expected n.freeCpus = 2, found %d", n.freeCpus)
	}
	if n.heap.Len() != 1 {
		t.Errorf("expected n.heap.Len() = 0, found %d", n.heap.Len())
	}
}

func TestDCHost(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
	}
	speed := [][]uint64{
		{0, 1},
		{1, 0},
	}
	t1 := sampleTask{
		end:  10,
		cpus: 2,
	}
	t2 := sampleTask{
		end:  20,
		cpus: 1,
	}

	topo, err := NewFifo(cap, speed)
	if err != nil {
		t.Errorf("expected err = nil, found %v", err)
	}
	dc1 := topo.DataCenters[0]
	n, success := dc1.Host(t1)
	if !success {
		t.Errorf("expected dc1.Host(2) = true, found %v", success)
	}
	if n != dc1.Get(0) {
		t.Errorf("expected node = dcl.nodes[0], found %v", n)
	}
	if free := dc1.Get(0).freeCpus; free != 0 {
		t.Errorf("expected dc1.nodes1.freeCpus = 0, found %d", free)
	}

	dc2 := topo.DataCenters[1]
	if _, success := dc2.Host(t1); success {
		t.Errorf("expected dc2.Host(2) = false, found %v", success)
	}

	dc2.Get(0).freeCpus = 0
	if n, success = dc2.Host(t2); n != dc2.Get(1) || !success {
		t.Errorf("expected dc2.Host(1) = dc2.node1, true, found %v, %v", n, success)
	}
}

func TestFree(t *testing.T) {
	n := NewNode(5, 0)

	n.Free(2)
	if n.freeCpus != 7 {
		t.Errorf("expected n.freeCpus = %v, found %v", 7, n.freeCpus)
	}
}

func testDC(t *testing.T, size, cpus int, dc DataCenter) {
	numNodes := dc.NumNodes()
	if numNodes != size {
		t.Errorf("wrong number of data centers created: expected %v, found %v", size, numNodes)
	}

	for i, node := range dc.Nodes() {
		if node.freeCpus != cpus {
			t.Errorf("wrong number of free cpus on node[%v]: expected %v, found %v", i, cpus, node.freeCpus)
		}
	}
}

func TestLoad(t *testing.T) {
	sample := "3\n2 1\n3 2\n4 3\n1000 99 200\n99 1000 500\n200 500 1000\n"
	reader := strings.NewReader(sample)
	topo, err := LoadFifo(reader)
	if err != nil {
		t.Fatalf("error '%v' while processing topology '%v', expected nil", err, sample)
	}

	numDC := len(topo.DataCenters)
	if numDC != 3 {
		t.Errorf("error while loading topology '%v': expected %v, found %v", sample, numDC, 2)
	}
	testDC(t, 2, 1, topo.DataCenters[0])
	testDC(t, 3, 2, topo.DataCenters[1])
	testDC(t, 4, 3, topo.DataCenters[2])

	speeds := [][]uint64{
		{1000, 99, 200},
		{99, 1000, 500},
		{200, 500, 1000},
	}
	if !cmp.Equal(speeds, topo.Speeds) {
		t.Errorf("error while loading topology '%v': expected dc.Speeds = %v, found %v", sample, speeds, topo.Speeds)
	}
}

func TestTopologyEqual(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
	}
	speed := [][]uint64{
		{0, 1},
		{1, 0},
	}
	fakeCap := [][2]int{
		{1, 2},
		{2, 2},
	}
	fakeSpeed := [][]uint64{
		{0, 2},
		{1, 0},
	}

	topo1, err := NewFifo(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	topo2, err := NewFifo(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	topo3, err := NewFifo(fakeCap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}
	topo4, err := NewFifo(cap, fakeSpeed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}

	if !topo1.Equal(*topo2) {
		t.Errorf("found %v.Equal(%v) == false, expected true", topo1, topo2)
	}
	if topo1.Equal(*topo3) {
		t.Errorf("found %v.Equal(%v) == false, expected true", topo1, topo3)
	}
	if topo1.Equal(*topo4) {
		t.Errorf("found %v.Equal(%v) == false, expected true", topo1, topo4)
	}
}

func TestDCAvailability(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
		{3, 5},
		{1, 3},
	}
	speed := [][]uint64{
		{0, 1, 1, 1},
		{1, 0, 1, 1},
		{1, 1, 0, 1},
		{1, 1, 1, 0},
	}
	task := sampleTask{
		end:  20,
		cpus: 4,
	}

	topo, err := NewFifo(cap, speed)
	if err != nil {
		t.Fatalf("failed to build topology: %v", err)
	}
	keys := map[int][]int{
		1: {2, 2, 15, 3},
		2: {1, 0, 6, 1},
	}
	for k, key := range keys {
		for i, dc := range topo.DataCenters {
			if cap := dc.JobAvailability(k); cap != key[i] {
				t.Errorf("wrong JobCapacity(%d) for dc[%d]: expected %d, found %d", k, i, key[i], cap)
			}
		}
	}
	topo.DataCenters[2].Host(task)
	postKeys := map[int][]int{
		1: {2, 2, 11, 3},
		2: {1, 0, 4, 1},
	}
	for k, key := range postKeys {
		for i, dc := range topo.DataCenters {
			if cap := dc.JobAvailability(k); cap != key[i] {
				t.Errorf("wrong JobCapacity(%d) for dc[%d]: expected %d, found %d", k, i, key[i], cap)
			}
		}
	}
}

func TestNodeEvent(t *testing.T) {
	tasks := []sampleTask{
		{end: 10, cpus: 1},
		{end: 5, cpus: 2},
	}
	n := NewNode(3, 0)
	n.Host(tasks[0])
	n.Host(tasks[1])
	if time := n.Time(); time != tasks[1].end {
		t.Fatalf("wrong time for node, expected %d, found %d", tasks[1].end, time)
	}
	if e := n.Process(); len(e) != 1 {
		t.Fatalf("wrong amount of returned events, expected 1, found %d", len(e))
	}

	if time := n.Time(); time != tasks[0].end {
		t.Fatalf("wrong time for node, expected %d, found %d", tasks[0].end, time)
	}
	if e := n.Process(); len(e) > 0 {
		t.Fatalf("wrong amount of returned events, expected none, found %v", e)
	}
}

// TODO: add tests for errors

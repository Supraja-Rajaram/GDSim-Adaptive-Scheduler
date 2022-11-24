package event

import (
	"testing"
)

type MockEvent struct {
	time    uint64
	new     []uint64
	results *[]uint64
}

func (mock MockEvent) Time() uint64 {
	return mock.time
}

func (mock MockEvent) Process() []Event {
	*mock.results = append(*mock.results, mock.time)
	if mock.new == nil {
		return nil
	}
	res := make([]Event, len(mock.new))
	for i, value := range mock.new {
		res[i] = MockEvent{
			time:    value,
			new:     nil,
			results: mock.results,
		}
	}
	return res
}

func TestSimulate(t *testing.T) {
	var results *[]uint64
	results = &[]uint64{}
	seeds := []Event{
		MockEvent{
			time:    2,
			new:     []uint64{4, 8, 5, 7},
			results: results,
		},
		MockEvent{
			time:    1,
			new:     []uint64{9, 6, 3},
			results: results,
		},
	}

	Simulate(seeds)

	expected := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(*results) != len(expected) {
		t.Fatalf("wrong number of processed events, expected %d, found %d", expected, *results)
	}
	for i := range expected {
		if expected[i] != (*results)[i] {
			t.Errorf("element %d wrong, expected %d, found %d", i, expected[i], (*results)[i])
		}
	}
}

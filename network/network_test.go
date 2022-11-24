package network

import "testing"

func TestNewSimpleNetwork(t *testing.T) {
	sn := NewSimpleNetwork()
	if len(sn.heap) != 0 || len(sn.connections) != 0 {
		t.Fatalf("NewSimpleNetwork does not return empty network")
	}
}

func check_connection(t *testing.T, sn SimpleNetwork, from, to string, speed, delay uint64) {
	conn, ok := sn.connections[from][to]
	if !ok {
		t.Fatalf("connection from %v to %v was not found in SimpleNetwork", from, to)
	}
	if conn.speed != speed {
		t.Errorf("connection from %v to %v has wrong speed: expected %v, found %v", from, to, speed, conn.speed)
	}
	if conn.delay != delay {
		t.Errorf("connection from %v to %v has wrong delay: expected %v, found %v", from, to, delay, conn.delay)
	}
}

func TestSNAddConnection(t *testing.T) {
	sn := NewSimpleNetwork()
	sn.AddConnection("0", "1", 1000, 10)
	check_connection(t, sn, "0", "1", 1000, 10)
	sn.AddConnection("0", "2", 1001, 12)
	check_connection(t, sn, "0", "2", 1001, 12)
	sn.AddConnection("1", "0", 999, 13)
	check_connection(t, sn, "1", "0", 999, 13)
}

func TestSNStartTransfer(t *testing.T) {
	t.Fatalf("test not implemented")
}

func TestSNAdvance(t *testing.T) {
	t.Fatalf("test not implemented")
}

func TestSNStatus(t *testing.T) {
	t.Fatalf("test not implemented")
}

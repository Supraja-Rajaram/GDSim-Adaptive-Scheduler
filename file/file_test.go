package file

import (
	"strings"
	"testing"

	"adaptive/topology"
	//"github.com/Supraja-Rajaram/gdsim_adaptive/topology"
	"github.com/google/go-cmp/cmp"
)

func TestLoad(t *testing.T) {
	cap := [][2]int{
		{1, 2},
		{2, 1},
	}
	speed := [][]uint64{
		{0, 1},
		{1, 0},
	}
	topo, err := topology.New(cap, speed)
	if err != nil {
		t.Fatalf("setup error: %v", err)
	}

	sample := "f1 2 0\nf2 6 0 1"
	key := map[string]File{
		"f1": {
			id:   "f1",
			size: 2,
		},
		"f2": {
			id:   "f2",
			size: 6,
		},
	}
	reader := strings.NewReader(sample)

	files, err := Load(reader, topo)
	if err != nil {
		t.Errorf("expected no error for sample '%v', found '%v'", sample, err)
	}
	if len(files) != 2 {
		t.Errorf("wrong size for files created by Load: expected %v, found %v", 2, len(files))
	}
	if !cmp.Equal(key, files) {
		t.Errorf("expected generated files equal to %v, found %v", key, files)
	}
}

func TestFileContainer(t *testing.T) {

	var fc FileContainer
	fc.Init()
	if l := len(fc.files); l != 0 {
		t.Fatalf("expected empty FileContainer, found len(fc.files) == %d", l)
	}
	files := []File{
		{
			id:   "f1",
			size: 1,
		},
		{
			id:   "f2",
			size: 1,
		},
		{
			id:   "f3",
			size: 3,
		},
		{
			id:   "f4",
			size: 3,
		},
	}
	for _, f := range files {
		fc.Add(f.id, f)
		if !fc.Has(f.Id()) {
			t.Fatalf("expected fc.Has(%s) to be true, found %v", f.Id(), fc.Has(f.Id()))
		}
		if g := fc.Find(f.Id()); !cmp.Equal(f, g) {
			t.Fatalf("expected fc.Find(%s) == %v, found %v", f.Id(), f, g)
		}
	}

	if l := len(fc.files); l != len(files) {
		t.Fatalf("expected len(fc.files) == %d, found %d", len(files), l)
	}

	p := fc.Pop("f3")
	if p != files[2] {
		t.Errorf("expected fc.Pop(%s) == %v, found %v", files[2].id, files[2], p)
	}
	if l := len(fc.files); l != len(files)-1 {
		t.Fatalf("expected len(fc.files) == %d, found %d", len(files)-1, l)
	}
}

package file

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	/*
		"github.com/Supraja-Rajaram/gdsim_adaptive/network"
		"github.com/Supraja-Rajaram/gdsim_adaptive/scheduler/event"
		"github.com/Supraja-Rajaram/gdsim_adaptive/topology"
	*/

	"adaptive/network"
	"adaptive/scheduler/event"
	"adaptive/topology"
)

type File struct {
	id       string
	size     uint64
	location []int // adaptive code
}

func New(id string, size uint64) File {
	return File{
		id:   id,
		size: size,
	}
}

// File getters for data members
func (f File) Id() string {
	return f.id
}

func (f File) Size() uint64 {
	return f.size
}

//adaptive code
func (f File) File_Location() []int {
	return f.location
}

// end of File Setters

// Comparison function for File using Id & Size
func (f File) Equal(d topology.Data) bool {
	return f.Id() == d.Id() && f.Size() == d.Size()
}

// SimpleFileDatabase implements Database of datacenter locations given a fileId.
type SimpleFileDatabase map[string][]string

// Constructor using builtin make()
func InitSimpleFileDatabase() SimpleFileDatabase {
	return make(map[string][]string)
}

// Access array of locations for a given file.
func (db SimpleFileDatabase) Location(fileId string) []string {
	return db[fileId]
}

// Records a file's datacenter location into SimpleFileDatabase
// If the fileId does not correspond to an already recorded file, a new entry will be created for it
func (db SimpleFileDatabase) Record(fileId, locationId string) {
	locationList, ok := db[fileId]
	if !ok {
		locationList = make([]string, 0)
	}
	db[fileId] = append(locationList, locationId)
}

// FileContainer implements the Container interface from the topology module
type FileContainer struct {
	id    string
	files map[string]File
	db    topology.Database
	nw    network.Network
}

// FileContainer setters for data members
func (fc *FileContainer) SetDatabase(db topology.Database) {
	fc.db = db
}

func (fc *FileContainer) SetNetwork(nw network.Network) {
	fc.nw = nw
}

// end of FileContainer setters

// Contructor for FileContainer
func (fc *FileContainer) Init(id string) {
	fc.id = id
	fc.files = make(map[string]File)
}

func (fc FileContainer) Add(id string, data topology.Data) {
	f := data.(File)
	fc.files[id] = f
	fc.db.Record(f.Id(), fc.id)
}

// this should not care what location the scheduler used to estimate,
// it should find the best one and transfer from there
func (fc FileContainer) Transfer(when uint64, fileId string, data topology.Data, consequence func(time uint64) []event.Event) []event.Event {
	f := data.(File)
	if _, ok := fc.files[fileId]; !ok {
		best := ""
		var bestStatus network.LinkStatus
		for _, location := range fc.db.Location(fileId) {
			status, err := fc.nw.Status(fc.id, location)
			if err != nil {
				// TODO: investigate if this is the best approach
				panic(err)
			}
			if best == "" || status.Bandwidth < bestStatus.Bandwidth {
				best = location
				bestStatus = status
			}
		}
		fc.nw.StartTransfer(when, f.size, best, fc.id, func(time uint64) []event.Event {
			fc.Add(fileId, data)
			return consequence(time)
		})
	} else {
		return consequence(when)
	}
	return nil
}

func (fc FileContainer) Has(id string) bool {
	_, ok := fc.files[id]
	return ok
}

func (fc FileContainer) Find(id string) topology.Data {
	return fc.files[id]
}

func (fc FileContainer) Pop(id string) topology.Data {
	f := fc.Find(id)
	delete(fc.files, id)
	return f
}

func Load(reader io.Reader, topo *topology.Topology, nw network.Network) (map[string]File, error) {
	res := make(map[string]File)
	containers := make([]FileContainer, len(topo.DataCenters))
	database := InitSimpleFileDatabase()
	for i := 0; i < len(containers); i++ {
		containers[i].Init(topo.DataCenters[i].Id())
		containers[i].SetDatabase(database)
		containers[i].SetNetwork(nw)
		topo.DataCenters[i].AddContainer(&containers[i])
	}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.Split(line, " ")
		s, err := strconv.ParseUint(words[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
		}
		f := New(words[0], s)
		f.location = make([]int, len(containers)) // adaptive code
		for i := 2; i < len(words); i++ {
			k, err := strconv.ParseInt(words[i], 0, 0)
			if err != nil {
				return nil, fmt.Errorf("failure to read file data %d: %v", len(res)+1, err)
			}
			//f.Locations[i-2] = int(k)
			containers[k].Add(f.Id(), f)
			topo.DataCenters[int(k)].Container().Add(f.Id(), f)
			f.location[k] = 1 // adaptive code
		}
		res[words[0]] = f
	}
	return res, nil
}

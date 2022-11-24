/*
The package job models a single job to be handled by the simulation, as well as the tasks that a job can have.
*/
package job

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	//"github.com/Supraja-Rajaram/gdsim_adaptive/file"

	"adaptive/file"
)

// A Task that is included in a Job.
type Task struct {
	Duration uint64
}

// A scheduled Task becomes DoneTask with Start time and Location of datacenter
type DoneTask struct {
	Start, Duration uint64
	Location        string
}

// A Job to be handled by the simulation with all its attributes.
type Job struct {
	Id                  string
	Submission          uint64
	Cpus                uint
	Tasks               []Task
	File                file.File
	Scheduled           []DoneTask
	Available_Cpu       int // adaptive code
	Total_task_duration int // adaptive code
}

/*
Loads a list of Jobs from a Reader, and requires a map of files to connect to names in Reader.
*/
func Load(reader io.Reader, files map[string]file.File) ([]Job, error) {
	scanner := bufio.NewScanner(reader)
	res := make([]Job, 0)

	// bandaid fix for larger lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	// TODO: need general fix for long lines
	var last uint64 = 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.Split(line, " ")
		if len(words) < 5 {
			return nil, fmt.Errorf("failure to read job %d: incomplete line", len(res)+1)
		}
		f, present := files[words[3]]
		if !present {
			return nil, fmt.Errorf("failure to read job %d: missing file %v", len(res)+1, words[3])
		}
		j := Job{
			Id:    words[0],
			File:  f,
			Tasks: make([]Task, 0),
		}
		cpus, err := strconv.ParseUint(words[1], 0, 0)
		if err != nil {
			return nil, fmt.Errorf("failure to read job %d: %v", len(res)+1, err)
		}
		j.Cpus = uint(cpus)
		j.Submission, err = strconv.ParseUint(words[2], 0, 64)
		j.Submission += last
		last = j.Submission
		if err != nil {
			return nil, fmt.Errorf("failure to read job %d: %v", len(res)+1, err)
		}
		for i := 4; i < len(words); i++ {
			d, err := strconv.ParseUint(words[i], 0, 64)
			if err != nil {
				return nil, fmt.Errorf("failure to read job %d: %v", len(res)+1, err)
			}
			t := Task{Duration: d}
			j.Tasks = append(j.Tasks, t)
			j.Total_task_duration += int(d) // adaptive code
		}
		res = append(res, j)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

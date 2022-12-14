# GDSim Adaptive Scheduler for Geo-distributed data centers

There are several scheduling algorithms available for geo-distributed data centers. The performance of a scheduling algorithm depends on resource availability and workload characteristics.
The main goal of adaptive scheduler is to learn the traffic trends observed in data centers and use that knowledge to improve the performance of job scheduling in geo-distributed data centers. 
Adaptive scheduler was developed using GDSim, an open-source job scheduling simulation tool for geo-distributed data centers. This scheduler adaptively chooses the best scheduling algorithm (SWAG and Geodis are used in this project) for a given batch of jobs using machine-learning techniques.

## Prerequisites

Run the following commands:

export CGO_CFLAGS="-I $(python3 -c 'import numpy; print(numpy.get_include())')"
pip3 install autogluon
go mod tidy

## How to use

Run the following commands from the root directory of this project.

### How to generate training data and train autogluon models

1. Run these commands for different workloads or add these commands in a makefile:

  go install gdsim.go <br />
  gdsim -files &lt;file path&gt; -topology &lt;topology path&gt; -scheduler ADAPTIVE -train &lt;output training data csv file&gt; &lt;job path&gt; <br />
  python3 utils/create_training_data.py &lt;folder containing csv files&gt; &lt;output csv file&gt; <br />
  gdsim -files &lt;file path&gt; -topology &lt;topology path&gt; -scheduler ADAPTIVE -train LEARN &lt;job path&gt; <br />

  (OR) <br />
  make -f  Makefile.train <br />

### How to run Adaptive Scheduler for different workloads

Run these commands for different workloads or add these commands in a makefile:

go install gdsim.go <br />
gdsim -files &lt;file path> -topology &lt;topology path&gt; -scheduler GEODIS &lt;job path&gt; > <output/workload_geodis> <br />
gdsim -files &lt;file path> -topology &lt;topology path&gt; -scheduler SWAG &lt;job path&gt; > <output/workload_swag> <br />
gdsim -files &lt;file path> -topology &lt;topology path&gt; -scheduler ADAPTIVE &lt;job path&gt; > <output/workload_adaptive> <br />
python3 utils/summarize_data.py <output/workload_geodis> >summary/workload_geodis <br />
python3 utils/summarize_data.py <output/workload_swag> >summary/workload_swag <br />
python3 utils/summarize_data.py <output/workload_adaptive> >summary/workload_adaptive <br />

(OR) <br />

make -f  Makefile.adaptive <br />

### How to evaluate the performance of Adaptive Scheduler

python3 utils/generate_report.py summary report.csv <br />
python3 utils/compare_results.py report.csv final_report.csv <br />


## Files format 

This section describe the format used in the files.
This format was selected for ease of initial implementation, but does not fully correspond to what can be implemented using the simulator's library.

### Job trace file format

Each line corresponds to a job, with five or more space separated fields:

 1. Job ID;
 2. Number of cores required for execution;
 3. Submission delay in seconds for this job, after the submission of the previous job (inter-arrival delay);
 4. File ID, for the file that is required for the execution of this job. The file is described in the file trace;
 5. 5th field and following: duration in seconds of each task required for the completion of the job.

### File trace file format

Each line corresponds to a file, with three or more space separated fields:

 1. File ID;
 2. Size of the file in bytes;
 3. 3rd and following: data centers that have a copy of the file. 0 means the first data center, 1 means the second, and so on. The highest number must not exceed the amount of available data centers

### Topology file format

The first line will have a single positive integer n, the number of data centers.
The next n lines will have each a pair of positive integer, the first for the number of computers in the corresponding data center, the second for the number of cores in each computer (while the simulator does not enforce that all computers have to be the same, This was simpler for the frontend).
Those are followed by another n lines, each of each containing n positive integers, forming an n by n matrix of bandwidth from one data center to another.
Bandwidth is measured in b/s.
The value indicating from a data center to itself is read but not used.

### Reference:

Please refer this for detailed instructions on how to use GDSim:
https://github.com/gdsim/gdsim 

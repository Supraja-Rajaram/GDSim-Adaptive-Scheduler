#!/usr/bin/python
'''
Example of how to process data from the simulation output in order to get statistics for further analyze.
'''

from sys import argv
import numpy as np

# Receives lists of tuples (start, expected, end) and list of ntasks per job
def summarize(identifier, tasks, jobs, latencies):
    '''
    Outputs a line of statistics derived from measurements of tasks, jobs and observed latencies. The identifier is added to the front of the line to identify the data.
    '''
    summary = "{0} {makespan} {ntasks} {mtasks} {mlatency} {p99latency} {stasks} {tdelay} {mdelay} {sdelay} {mduration} {sduration} {marrival} {sarrival}"
    data = {}
    tasks.sort()
    delays = [x[0] - x[1] for x in tasks]
    durations = [x[2] - x[0] for x in tasks]
    arrivals = np.diff([x[0] for x in tasks])
    end = np.max([x[2] for x in tasks])
    data['makespan'] = end - tasks[0][0]
    data['ntasks'] = np.sum(jobs)
    data['mtasks'] = np.mean(jobs)
    data['stasks'] = np.std(jobs)
    data['tdelay'] = np.sum(delays)
    data['mdelay'] = np.mean(delays)
    data['sdelay'] = np.std(delays)
    data['mduration'] = np.mean(durations)
    data['sduration'] = np.std(durations)
    data['marrival'] = np.mean(arrivals)
    data['sarrival'] = np.std(arrivals)
    data['mlatency'] = np.mean(latencies)
    data['p99latency'] = np.percentile(latencies, 99)
    print(summary.format(identifier, **data))
    
    """
    print("Durations: ")
    for i in durations:
    	print(i)
    """

def main(resultfile):
    tasks = []
    jobs = []
    latencies = []
    with open(resultfile) as data:
        data.readline()
        for line in data:
            _, original, readtasks = line.split(maxsplit=2)
            readtasks = eval(readtasks)
            jobs.append(len(readtasks))
            original = int(original)
            last = original
            for _, _, _, start, end in readtasks:
                tasks.append((start, original, end))
                if end > last:
                    last = end
            latencies.append(last - original)
    summarize(resultfile, tasks, jobs, latencies)


if __name__ == "__main__":
    print("identifier makespan num_tasks mean_task_num mean_job_latency p99_job_latency std_job_latency total_delay mean_delay std_delay mean_duration std_duration mean_arrival std_arrival")
    for filename in argv[1:]:
        main(filename)

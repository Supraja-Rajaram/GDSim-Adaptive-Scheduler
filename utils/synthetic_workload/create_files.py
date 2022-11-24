#!/usr/bin/env python

from random import sample
from scipy.stats import pareto
from sys import argv

def get_tasknum(jobfile):
    d = {}
    with open(jobfile) as f:
        for line in f:
            parts = line.strip().split()
            d[parts[3]] = len(parts[4:])
    return d



def print_files(num, max_dc, pareto_param, jobfile):
    count = 0
    num_replicas = 3
    tasknum = get_tasknum(jobfile)
    for val in range(num):
        n = val + 1
        name = f"file{n}"
        #size = 10000
        size = int(pareto.rvs(2) * pareto_param) // tasknum[name]
        if size == 0:
            size = 1
        locations = sample(range(max_dc), num_replicas)
        print(f"{name} {size} " + " ".join(str(x) for x in locations))

if __name__ == "__main__":
    if len(argv) < 3:
        print(f"missing argument: {argv[0]} pareto_param jobfile")
        exit()
    pareto_param = float(argv[1])
    jobfile = argv[2]
    print_files(3000, 8, pareto_param, jobfile)

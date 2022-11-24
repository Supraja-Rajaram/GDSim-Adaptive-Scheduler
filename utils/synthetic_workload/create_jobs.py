#!/usr/bin/env python3
from numpy import random
from sys import argv
from scipy.stats import uniform, randint

def elephant_tasks():
    num_tasks = randint.rvs(low=1, high=5)      # no of tasks
    return randint.rvs(low=1, high=30000, size=num_tasks)  # duration of each task

def mice_tasks():
    num_tasks = randint.rvs(low=10, high=200)
    return randint.rvs(low=1, high=50000, size=num_tasks)

def generate(num, elephant_rate=0.2):
    count = 0
    for val in uniform.rvs(size=num):
        count += 1
        #n = random.randint(1,1001) # i added
        t = random.randint(1,2)      # inter arrival duration
        
        #description = f"job{count} 1 0 file{count} "
        description = f"job{count} 1 {t} file{count} "     # i added
        if val < elephant_rate:
            tasks = elephant_tasks()
        else:
            tasks = mice_tasks()
        description += " ".join(str(x) for x in tasks)
        print(description)

if __name__ == "__main__":
    if len(argv) < 2:
        print("Missing arguments")
        exit(1)
    rate = float(argv[1])
    generate(3000, rate)          # no of jobs, ratio of elephant jobs (elephant jobs/total jobs)

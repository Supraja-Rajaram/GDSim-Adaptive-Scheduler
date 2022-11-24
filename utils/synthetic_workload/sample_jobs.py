import sys
import os
import pandas as pd
import random

def sample_job(filename, sampled_filename):

    # Read the lines from the source job file and save them in a list (lines)
    with open(filename) as f:
        lines = f.readlines()
    
    sampled_job_list = []
    # Select jobs from lines randomly and add them to new list- sampled_job_list
    while len(lines) != 0:
        random_index = random.randint(0,len(lines)-1)
        sampled_job_list.append(lines[random_index])
        lines.remove(lines[random_index])

    # Write the contents of sampled_job_list to a new file
    with open(sampled_filename, 'w') as fp:
        for job in sampled_job_list:
            fp.write("%s" % job)
'''
To run the script:
python3 sample_jobs.py <source job filename> <name of output job file>
'''
if __name__ == "__main__":

    job_file = sys.argv[1]
    sampled_filename = sys.argv[2]

    # Should enter into the directory to access the files
    sample_job(job_file,sampled_filename)
  
import sys
import os
import pandas as pd

def read_files(filename, df):
    
    with open(filename) as f:
        lines = f.readlines()

    values = lines[1].split()

    '''
    Identifier should be in following format : 
    <directory name>/workload_scheduler
    eg: output/fb_adaptive
    workload -> fb
    scheduler -> adaptive
    '''
    identifier = values[0]
    filename = identifier.split('/')      
    scheduler = filename[1].split('_')
    workload = scheduler[0]
    makespan = values[1]
    mean_job_latency = values[4]
    p99_job_latency = values[5]
 
    df.loc[len(df.index)] = [identifier, workload, scheduler[1], makespan, mean_job_latency, p99_job_latency]

    return df
'''
To run the script:
python3 generate_report.py <dir containing summary files> <name of output file>
'''
if __name__ == "__main__":

    df = pd.DataFrame(columns=['Filename', 'Workload', 'Scheduler', 'Makespan','Mean Job Latency', 'P99 Job Latency'])

    directory = sys.argv[1]
    output_file = sys.argv[2]

    file_list = os.listdir(directory)
    # Should enter into the directory to access the files
    os.chdir(directory)
    for file in file_list:
    	#print(file)
    	df = read_files(file,df)
    '''
    # Iterate over all files provided in cmd
    for filename in sys.argv[1:]:
        read_files(filename,df)
    '''
    os.chdir("..")
    df.to_csv(output_file, index=False)

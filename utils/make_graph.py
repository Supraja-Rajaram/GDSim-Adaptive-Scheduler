import sys
import pandas as pd
import matplotlib.pyplot as plt

def organise_makespan(report_file,report_df):
    df = pd.read_csv(report_file)
    df = df.sort_values('Workload', ascending=True)

    df2 = df.groupby("Workload")

    for _,df_group in df2:
        makespan = [0,0,0]
        workload =""
        for index,row in df_group.iterrows():
            if row['Scheduler'] == 'swag':
                makespan[0] = row['Makespan']
            elif row['Scheduler'] == 'geodis':
                makespan[1] = row['Makespan']
            else:
                makespan[2] = row['Makespan']
                workload = row['Workload']

        df2 = pd.DataFrame([[workload,makespan[0],makespan[1],makespan[2] ]], columns=['WORKLOAD','SWAG','GEODIS','ADAPTIVE'])
        report_df = pd.concat([df2, report_df])

    return report_df

def organise_mean_latency(report_file,report_df):
    df = pd.read_csv(report_file)
    df = df.sort_values('Workload', ascending=True)

    df2 = df.groupby("Workload")

    for _,df_group in df2:
        makespan = [0,0,0]
        workload =""
        for index,row in df_group.iterrows():
            if row['Scheduler'] == 'swag':
                makespan[0] = row['Mean Job Latency']
            elif row['Scheduler'] == 'geodis':
                makespan[1] = row['Mean Job Latency']
            else:
                makespan[2] = row['Mean Job Latency']
                workload = row['Workload']

        df2 = pd.DataFrame([[workload,makespan[0],makespan[1],makespan[2] ]], columns=['WORKLOAD','SWAG','GEODIS','ADAPTIVE'])
        report_df = pd.concat([df2, report_df])

    return report_df



def organise_p99_latency(report_file,report_df):
    df = pd.read_csv(report_file)
    df = df.sort_values('Workload', ascending=True)

    df2 = df.groupby("Workload")

    for _,df_group in df2:
        makespan = [0,0,0]
        workload =""
        for index,row in df_group.iterrows():
            if row['Scheduler'] == 'swag':
                makespan[0] = row['P99 Job Latency']
            elif row['Scheduler'] == 'geodis':
                makespan[1] = row['P99 Job Latency']
            else:
                makespan[2] = row['P99 Job Latency']
                workload = row['Workload']

        df2 = pd.DataFrame([[workload,makespan[0],makespan[1],makespan[2] ]], columns=['WORKLOAD','SWAG','GEODIS','ADAPTIVE'])
        report_df = pd.concat([df2, report_df])

    return report_df

'''
To run the script:
python3 make_graph.py <report file> <name of output file> <metric>
Metric: makespan, mean_job_latency, p99_job_latency
'''
if __name__ == "__main__":
    report_file = sys.argv[1]
    output_file = sys.argv[2]
    metric = sys.argv[3]

    country = ['INDIA', 'JAPAN', 'CHINA', 'USA', 'GERMANY']
    population = [1000,800,600,400,1100]
    plt.bar(country,population)
    plt.show()
    plt.savefig("sample.png")
    
    report_df = pd.DataFrame(columns=['WORKLOAD','SWAG','GEODIS','ADAPTIVE'])
    if metric == 'makespan':
        report_df = organise_makespan(report_file,report_df)
    elif metric == 'mean_job_latency' :
        report_df = organise_mean_latency(report_file,report_df)
    else:
        report_df = organise_p99_latency(report_file,report_df)
    report_df = report_df.sort_values('WORKLOAD', ascending=True)
    report_df.to_csv(output_file, index=False)

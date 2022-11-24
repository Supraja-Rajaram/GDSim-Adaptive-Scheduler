import sys
import pandas as pd

def compare_result(report_file, report_df):
    df = pd.read_csv(report_file)
    df2 = df.groupby("Workload")
    for workload,rows in df2:
        sorted_df = rows.sort_values(by=['Makespan'], ascending=True)
        best_makespan = sorted_df.iloc[0]['Scheduler']
        second_best_makespan = sorted_df.iloc[1]['Scheduler']
        
        sorted_df = rows.sort_values(by=['Mean Job Latency'], ascending=True)
        best_mean_latency = sorted_df.iloc[0]['Scheduler']
        second_best_mean_latency = sorted_df.iloc[1]['Scheduler']

        sorted_df = rows.sort_values(by=['P99 Job Latency'], ascending=True)
        best_p99_latency = sorted_df.iloc[0]['Scheduler']
        second_best_p99_latency = sorted_df.iloc[1]['Scheduler']

        report_df.loc[len(report_df.index)] = [workload, best_makespan, second_best_makespan,
                                best_mean_latency, second_best_mean_latency,
                                best_p99_latency, second_best_p99_latency]

    return report_df

'''
To run this script:
python3 compare_results.py <report csv file> <output file>
'''
if __name__ == "__main__":
    report_file = sys.argv[1]
    output_file = sys.argv[2]

    report_df = pd.DataFrame(columns=['Workload', 'Best Makespan' ,'Second best Makespan', 
                                      'Best mean job Latency', 'Second best mean job latency' ,
                                      'Best p99 job latency', 'Second best p99 job latency'])
    
    report_df = compare_result(report_file,report_df)

    report_df.to_csv(output_file, index=False)

    rows_count = report_df.shape[0]
    #print(report_df['Best Makespan'].value_counts(ascending=False))
    print('\n',"*************Summary*************",'\n')
    print("Total workloads: ", rows_count)
    print(report_df.groupby('Best Makespan').size().to_string(),'\n')
    print(report_df.groupby('Second best Makespan').size().to_string(),'\n')
    print(report_df.groupby('Best mean job Latency').size().to_string(),'\n')
    print(report_df.groupby('Second best mean job latency').size().to_string(),'\n')
    print(report_df.groupby('Best p99 job latency').size().to_string(),'\n')
    print(report_df.groupby('Second best p99 job latency').size().to_string(),'\n')

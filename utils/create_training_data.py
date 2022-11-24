import os
import sys
import pandas as pd

def create_data(directory):

    file_list = os.listdir(directory)
    # Should enter into the directory to access the files
    os.chdir(directory)

    df_append = pd.DataFrame(columns=['Total_jobs', 'Total_tasks', 'Total_task_duration', 'Required_CPU','Available_CPU','Scheduler'])

    #append all files together
    for file in file_list:
        print(file)
        df_temp = pd.read_csv(file)
        df_temp.columns = ['Total_jobs', 'Total_tasks', 'Total_task_duration', 'Required_CPU','Available_CPU','Scheduler']
        df_append = pd.concat([df_append, df_temp], ignore_index=True)

    cols = ['Total_jobs', 'Total_tasks', 'Total_task_duration', 'Required_CPU','Available_CPU']
    df_append[cols] = df_append[cols].apply(pd.to_numeric, errors='coerce', axis=1)
    
    os.chdir("..")

    return df_append

'''
To run the script:
python3 create_training_data.py <directory with csv files> <output file>
'''   

if __name__ == "__main__":

    directory = sys.argv[1]
    output_file = sys.argv[2]

    df_append = create_data(directory)

    df_append.to_csv(output_file,index=False)

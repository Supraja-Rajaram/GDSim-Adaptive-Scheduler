import pandas as pd
import numpy as np
from autogluon.tabular import TabularDataset, TabularPredictor

count = 0
# Learn training data using Autogluon
def learn_data(something):
    
	train_data = TabularDataset("all_training_data.csv")
	save_path = "ml/agModels"
	predictor = TabularPredictor(label='Scheduler', path=save_path).fit(train_data=train_data)
	print(predictor.leaderboard(train_data, silent=True))
	results = predictor.fit_summary(show_plot=True)
	#print(results)
	
	return 1

# Predict optimal scheduler from trained model
def predict_autogluon(data):
	
	df = pd.DataFrame(columns = ['Total_jobs', 'Total_tasks', 'Total_task_duration', 'Required_CPU', 'Available_CPU'])

	df.loc[len(df.index)] = [int(data[0]), int(data[1]),int(data[2]), int(data[3]), int(data[4]) ]
	# To predictor only once
	global count
	global predictor
	if count == 0:
		predictor = TabularPredictor.load("ml/agModels")
		count = count+1
	predictions = predictor.predict(df)

	index = 0
	if predictions[0] == "SWAG":
		index = 0
	else:
		index = 1
	result = np.array([index])

	return result
 

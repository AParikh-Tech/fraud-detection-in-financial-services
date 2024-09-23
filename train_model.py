import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import json

# Load the dataset
data = pd.read_csv('fraudTrain.csv')

# Preprocess the data
# Convert the transaction time to a numerical feature (you may want to use a better feature engineering method)
data['trans_date_trans_time'] = pd.to_datetime(data['trans_date_trans_time'])
data['time_of_day'] = data['trans_date_trans_time'].dt.hour + data['trans_date_trans_time'].dt.minute / 60.0

# One-hot encode categorical features
data = pd.get_dummies(data, columns=['merchant', 'category', 'state'], drop_first=True)

# Define the features and target variable
features = [
    'amt',
    'time_of_day',
    'city_pop',
    'lat',
    'long'  # You may choose to add more features as needed
]

target = 'is_fraud'

X_train = data[features]
y_train = data[target]

# Create an XGBoost DMatrix for the training data
dtrain = xgb.DMatrix(X_train, label=y_train)

# Define the parameters for the XGBoost model
params = {
    'objective': 'binary:logistic',
    'eval_metric': 'logloss',
    'learning_rate': 0.1,
    'max_depth': 6,
    'alpha': 10,
    'lambda': 1,
}

# Train the model
bst = xgb.train(params, dtrain, num_boost_round=100)

# Save the model
bst.save_model('fraud_detection.model')

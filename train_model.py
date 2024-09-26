import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import json

# Load the dfset
df = pd.read_csv('train_data.csv')

# Drop unneccary columns
df = df.drop(['cc_num'], axis=1)

# Preprocess the df
# Convert the transaction time to a numerical feature 
df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
df['time_of_day'] = df['trans_date_trans_time'].dt.hour + df['trans_date_trans_time'].dt.minute / 60.0

# Define the features and target variable
features = [
    'amt',
    'time_of_day',
    'city_pop',
    'lat',
    'long',
    'merchant',
    'category',
    'state'
]

X_train = df[features]
y_train = df['is_fraud']

# One-hot encode categorical features
X_train = pd.get_dummies(X_train, columns=['merchant', 'category', 'state'], drop_first=True)

print(X_train)
# Create an XGBoost DMatrix for the training df
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

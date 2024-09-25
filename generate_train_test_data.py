import pandas as pd

# Load the original dataset
df = pd.read_csv('fraudTrain.csv')

# Split the dataset into training and test datasets
train_df = df.sample(frac=0.8, random_state=42)
test_df = df.drop(train_df.index)

# Save the datasets
train_df.to_csv('training_data.csv', index=False)
test_df.to_csv('test_data.csv', index=False)


import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Define the number of rows to generate
num_rows = 500000

# Create lists of possible values for various columns
sports = ['Football', 'Basketball', 'Soccer', 'Tennis', 'Baseball']
outcomes = ['Win', 'Loss']
bet_types = ['Moneyline', 'Point Spread', 'Over/Under']

# Generate data for each column
user_ids = np.random.randint(1000, 50000, num_rows)
bet_ids = range(1, num_rows + 1)
bet_amounts = np.random.uniform(5, 500, num_rows).round(2)
outcomes_data = random.choices(outcomes, k=num_rows, weights=[0.45, 0.55]) # 45% win rate
sports_data = random.choices(sports, k=num_rows, weights=[0.4, 0.2, 0.2, 0.1, 0.1])
bet_types_data = random.choices(bet_types, k=num_rows)

# Generate timestamps over the last year
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
timestamps = [start_date + timedelta(days=random.randint(0, 364), seconds=random.randint(0, 86399)) for _ in range(num_rows)]

# Calculate payouts and profit/loss
payouts = []
profit_loss = []
for amount, outcome in zip(bet_amounts, outcomes_data):
    if outcome == 'Win':
        payout = amount * random.uniform(1.1, 5.0)
        payouts.append(payout)
        profit_loss.append(payout - amount)
    else:
        payouts.append(0)
        profit_loss.append(-amount)

# Create the DataFrame
data = pd.DataFrame({
    'bet_id': bet_ids,
    'user_id': user_ids,
    'bet_amount': bet_amounts,
    'outcome': outcomes_data,
    'sport': sports_data,
    'bet_type': bet_types_data,
    'payout': payouts,
    'profit_loss': profit_loss,
    'bet_timestamp': timestamps
})

# Save to a CSV file
data.to_csv('sports_betting_data.csv', index=False)
print("Simulated data has been generated and saved to sports_betting_data.csv")

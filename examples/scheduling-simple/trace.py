import matplotlib.pyplot as plt
import pandas as pd

# Sample data (replace this with your actual data)
data = pd.read_csv('machines.txt')


data.sort_values('start')


print(data.columns)

max_time = 15e9

# Extracting IDs, start times, and end times from the data
ids = data['id']
start_times = data['start']
end_times = data['end']

# Plotting
plt.figure(figsize=(10, 6))

# Plot horizontal lines for each row
for i in range(len(ids)):
    plt.plot([start_times[i], end_times[i] if end_times[i] is not None else max_time], [i + 0.1, i + 0.1], linewidth=2)

# Setting labels and title
plt.xlabel('Time')
plt.ylabel('ID')
plt.title('Horizontal Lines Plot')
# plt.yticks(ids)  # Set y-axis ticks to the IDs
# plt.grid(True)

# Show plot
plt.savefig("lines.png")



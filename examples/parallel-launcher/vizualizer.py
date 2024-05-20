import matplotlib.pyplot as plt
import numpy as np

# Read data from the file
with open('load.txt', 'r') as file:
    data = file.readlines()

with open('scheduler_info.txt', 'r') as file:
    scheduler_info = file.readlines()

# Parse the data into separate lists
times = []
host_names = []
cpu_usages = []
memory_usages = []

for line in data:
    parts = line.strip().split()
    times.append(float(parts[0]))
    host_names.append(parts[1])
    cpu_usages.append(float(parts[2]))
    memory_usages.append(float(parts[3]))

# Convert lists to numpy arrays
times = np.array(times)
cpu_usages = np.array(cpu_usages)
memory_usages = np.array(memory_usages)
host_names = np.array(host_names)

# Get unique host names
unique_host_names = np.unique(host_names)


def plot_data(prefix: str):
    # Create a figure with a grid of subplots
    cur_names = list(filter(lambda x: x.startswith(prefix),unique_host_names))
    if len(cur_names) == 0:
        return
    num_rows = len(cur_names)
    fig, axes = plt.subplots(max(num_rows, 2), 2, figsize=(25, 5 * num_rows), sharex='col')

    # Loop through each host name
    for row, host_name in enumerate(cur_names):

        # Plot CPU usage
        indices = np.where((host_names == host_name))
        axes[row, 0].plot(times[indices], cpu_usages[indices], label=f'{host_name}', linestyle='-', marker='')
        axes[row, 0].set_ylabel('CPU Usage')
        axes[row, 0].set_title(f'{host_name} - CPU Usage')
        axes[row, 0].legend()

        axes[row, 1].plot(times[indices], memory_usages[indices], label=f'{host_name}', linestyle='-', marker='')
        axes[row, 1].set_ylabel('Memory Usage')
        axes[row, 1].set_title(f'{host_name} - Memory Usage')
        axes[row, 1].legend()

    # Set common x-axis label
    axes[-1, 0].set_xlabel('Time')
    axes[-1, 1].set_xlabel('Time')

    # Adjust layout
    plt.tight_layout()

    # Save the plot
    plt.savefig(f'{prefix}_timeseries.png')

plot_data('host')
plot_data('group')
plot_data('TOTAL')


plt.clf()
plt.plot(figsize=(25, 5))
plt.xlabel('Time')
plt.ylabel('Queue Size')
plt.title('Scheduler Queue Size')
times = []
queue_sizes = []
for line in scheduler_info:
    parts = line.strip().split()
    times.append(float(parts[0]))
    queue_sizes.append(float(parts[1]))

plt.plot(times, queue_sizes, label='Queue Size', linestyle='-', marker='')
plt.savefig('queue_size.png')

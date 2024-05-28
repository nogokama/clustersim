import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 

# Read data from the file
data = pd.read_csv('load.txt')
scheduler_info = pd.read_csv('scheduler_info.txt')
fair_share_info = pd.read_csv('fair_share.txt')


# Get unique host names
unique_host_names = np.unique(data['name'])


def plot_data(prefix: str):
    # Create a figure with a grid of subplots
    cur_names = list(filter(lambda x: x.startswith(prefix),unique_host_names))
    print(f'Plotting {prefix} data...')
    if len(cur_names) == 0:
        return
    num_rows = len(cur_names)
    fig, axes = plt.subplots(max(num_rows, 2), 2, figsize=(10, 5 * num_rows), sharex='col')

    # Loop through each host name
    for row, host_name in enumerate(cur_names):

        # Plot CPU usage
        times = data[data['name'] == host_name]['time']
        cpu_usages = data[data['name'] == host_name]['cpu_load']
        memory_usages = data[data['name'] == host_name]['memory_load']

        axes[row, 0].plot(times, cpu_usages, label=f'{host_name}', linestyle='-', marker='')
        axes[row, 0].set_ylabel('CPU Usage')
        axes[row, 0].set_ylim(0, 1)
        axes[row, 0].set_title(f'{host_name} - CPU Usage')
        axes[row, 0].legend()

        axes[row, 1].plot(times, memory_usages, label=f'{host_name}', linestyle='-', marker='')
        axes[row, 1].set_ylabel('Memory Usage')
        axes[row, 1].set_ylim(0, 1)
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

### QUEUE 


plt.clf()
plt.plot(figsize=(10, 5))
plt.xlabel('Time')
plt.ylabel('Queue Size')
plt.title('Scheduler Queue Size')



times = scheduler_info['time']
queue_sizes = scheduler_info['queue_size']
users = scheduler_info['user']

unique_users = np.unique(users)

for user in unique_users:
    print(user)
    times = scheduler_info[scheduler_info['user'] == user]['time']
    queue_sizes = scheduler_info[scheduler_info['user'] == user]['queue_size']
    plt.plot(times, queue_sizes, label=f'{user}', linestyle='-', marker='')

plt.legend()
plt.savefig('queue_size.png')


### METRICS 

# plt.clf()

# times = np.array(times)
# host_names = np.array(host_names)
# cpu_usages = np.array(cpu_usages)
# memory_usages = np.array(memory_usages)

# indices = np.where((host_names == 'TOTAL') & (times > 1000) & (times < 5000))

# cpu_utilization = np.mean(cpu_usages[indices])
# memory_utilization = np.mean(memory_usages[indices])

# print(f"CPU Utilization: {cpu_utilization}")
# print(f"Memory Utilization: {memory_utilization}")

# print(f"( , {cpu_utilization}, {memory_utilization})")
# # total_utilization = cpu_usages[np.where(host_names == 'TOTAL')], memory_usages[np.where(host_names == 'TOTAL')]
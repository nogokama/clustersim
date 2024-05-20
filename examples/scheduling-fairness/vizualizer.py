import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 


# Read data from the file
data = pd.read_csv('load.txt')
scheduler_info = pd.read_csv('scheduler_info.txt')
fair_share_info = pd.read_csv('fair_share.txt')




# Get unique host names
unique_host_names = np.array(list(data['name'].value_counts().keys()))


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

unique_users = np.array(list(set(users)))

for user in unique_users:
    print(user)
    times = scheduler_info[scheduler_info['user'] == user]['time']
    queue_sizes = scheduler_info[scheduler_info['user'] == user]['queue_size']
    plt.plot(times, queue_sizes, label=f'{user}', linestyle='-', marker='')

plt.legend()
plt.savefig('queue_size.png')


##### FAIR SHARE 

plt.clf()
plt.figure(figsize=(6, 4))
# plt.title("Разделение ресурсов кластера (f = 1.0)")
plt.xlabel('Время симуляции (секунды)')
plt.ylabel('Доминантная доля пользователя')
plt.ylim(0, 1)

ax = plt.gca()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.tick_params(axis='both', which='both', length=0)
plt.grid(color='white')
plt.gca().set_facecolor((0.93, 0.93, 0.93))

fair_share_info = pd.read_csv('fair_share.txt')

times = np.array(fair_share_info['time'])
shares = np.array(fair_share_info['share'])
users = np.array(fair_share_info['user'])

unique_users = np.array(list(filter(lambda u: isinstance(u, str), set(users))))   

for user in unique_users:
    indices = np.where((users == user))
    plt.plot(times[indices], shares[indices], label=f'{user}', linestyle='-', marker='')

plt.legend()
plt.savefig('fair_share.png', dpi=300)


### METRICS 

plt.clf()


data = pd.read_csv('load.txt')


times = np.array(data['time'])
host_names = np.array(data['name'])
cpu_usages = np.array(data['cpu_load'])
memory_usages = np.array(data['memory_load'])

indices = np.where((host_names == 'TOTAL') & (times > 1000) & (times < 5000))

cpu_utilization = np.mean(cpu_usages[indices])
memory_utilization = np.mean(memory_usages[indices])

print(f"CPU Utilization: {cpu_utilization}")
print(f"Memory Utilization: {memory_utilization}")

print(f"( , {cpu_utilization}, {memory_utilization})")
# total_utilization = cpu_usages[np.where(host_names == 'TOTAL')], memory_usages[np.where(host_names == 'TOTAL')]
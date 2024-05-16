import matplotlib.pyplot as plt
import numpy as np

# Read data from the file
with open('load.txt', 'r') as file:
    data = file.readlines()

with open('scheduler_info.txt', 'r') as file:
    scheduler_info = file.readlines()

with open('fair_share.txt', 'r') as file: 
    fair_share_info = file.readlines()


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
    print(f'Plotting {prefix} data...')
    if len(cur_names) == 0:
        return
    num_rows = len(cur_names)
    fig, axes = plt.subplots(max(num_rows, 2), 2, figsize=(10, 5 * num_rows), sharex='col')

    # Loop through each host name
    for row, host_name in enumerate(cur_names):

        # Plot CPU usage
        indices = np.where((host_names == host_name))
        axes[row, 0].plot(times[indices], cpu_usages[indices], label=f'{host_name}', linestyle='-', marker='')
        axes[row, 0].set_ylabel('CPU Usage')
        axes[row, 0].set_ylim(0, 1)
        axes[row, 0].set_title(f'{host_name} - CPU Usage')
        axes[row, 0].legend()

        axes[row, 1].plot(times[indices], memory_usages[indices], label=f'{host_name}', linestyle='-', marker='')
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
times = []
queue_sizes = []
users = []
for line in scheduler_info:
    parts = line.strip().split()
    times.append(float(parts[0]))
    queue_sizes.append(float(parts[1]))
    users.append(str(parts[2]))

times = np.array(times)
queue_sizes = np.array(queue_sizes)
users = np.array(users)

unique_users = np.unique(users)

for user in unique_users:
    print(user)
    indices = np.where((users == user))
    plt.plot(times[indices], queue_sizes[indices], label=f'{user}', linestyle='-', marker='')

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

times = []
users = []
shares = []
for line in fair_share_info: 
    parts = line.strip().split()
    times.append(float(parts[0]))
    users.append(str(parts[1]))
    shares.append(float(parts[2]))

times = np.array(times)
shares = np.array(shares)
users = np.array(users)

unique_users = np.unique(users)

for user in unique_users:
    indices = np.where((users == user))
    plt.plot(times[indices], shares[indices], label=f'{user}', linestyle='-', marker='')

plt.legend()
plt.savefig('fair_share.png', dpi=300)


### METRICS 

plt.clf()

with open('load.txt', 'r') as file:
    data = file.readlines()

host_names = [] 
cpu_usages = []
memory_usages = []
times = []

for line in data:
    parts = line.strip().split()
    times.append(float(parts[0]))
    host_names.append(parts[1])
    cpu_usages.append(float(parts[2]))
    memory_usages.append(float(parts[3]))

times = np.array(times)
host_names = np.array(host_names)
cpu_usages = np.array(cpu_usages)
memory_usages = np.array(memory_usages)

indices = np.where((host_names == 'TOTAL') & (times > 1000) & (times < 5000))

cpu_utilization = np.mean(cpu_usages[indices])
memory_utilization = np.mean(memory_usages[indices])

print(f"CPU Utilization: {cpu_utilization}")
print(f"Memory Utilization: {memory_utilization}")

print(f"( , {cpu_utilization}, {memory_utilization})")
# total_utilization = cpu_usages[np.where(host_names == 'TOTAL')], memory_usages[np.where(host_names == 'TOTAL')]
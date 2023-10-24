import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from matplotlib.ticker import MaxNLocator

num = 3
scn = 1

bw_values = []
time_values = []

path_name = f'./plots/scenario{scn}'
os.makedirs(path_name, exist_ok=True)

for i in range(1, num + 1):
    file = f'./stats/scenarios{scn}/node{i}_stats.csv'
    stats_data = pd.read_csv(file).to_numpy()
    
    if len(stats_data) == 0:
        raise ValueError('No CSV data!')
    
    messages = stats_data[:, 0]
    message_count = len(messages)
    receive_times = stats_data[:, 1].astype(float)
    process_times = stats_data[:, 2].astype(float)
    total_bytes = stats_data[:, 3].astype(int)

    total_time_diff = np.sum(process_times - receive_times)
    avg_time = total_time_diff / message_count * 1000
    bandwidth = 8 * (total_bytes[-1] - total_bytes[0]) / (process_times[-1] - receive_times[0])

    time_values.append(avg_time)
    bw_values.append(bandwidth)

fig1 = plt.figure()
plt.plot(range(1, num + 1), time_values)
plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
plt.title('Average Delay')
plt.xlabel('Node')
plt.ylabel('Delay per message [ms]')
fig1.savefig(f'{path_name}/delay.png', dpi=fig1.dpi)
plt.show()

fig2 = plt.figure()
plt.plot(range(1, num + 1), bw_values)
plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
plt.title('Bandwidth')
plt.xlabel('Node')
plt.ylabel('Bandwidth [bps]')
fig2.savefig(f'{path_name}/bandwidth.png', dpi=fig2.dpi)
plt.show()

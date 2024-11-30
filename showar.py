#!/usr/bin/env python3
import collections
import datetime
import numpy as np
import os
import pathlib
import time

from typing import Any, Dict, List, Set


def get_ctr_map(components):
    ctr_map = {}
    for name in components:
        ctr_map[name] = f'docker/{name}/'
    return ctr_map

def stat_path(ctr_map, name, stat):
    group = ctr_map[name]
    return pathlib.Path(f'/sys/fs/cgroup/cpu/{group}/{stat}')

def set_cpu_limit(ctr_map, name, limit, period=0.1):
    period_us = round(period * 1e6)
    assert 1000 <= period_us <= 1000000
    if limit is None:
        quota_us = -1
    else:
        quota_us = round(limit * period_us)
        assert quota_us >= 1000

    stat_path(ctr_map, name, 'cpu.cfs_period_us').write_text(str(period_us))
    stat_path(ctr_map, name, 'cpu.cfs_quota_us').write_text(str(quota_us))
    print(f'{datetime.datetime.now()} Written period={period_us},quota={quota_us} to name={name},(qos,uid)={ctr_map[name]}')

    return 

def get_running_containers(root_dir: str):
    # traverse root directory, and list directories as dirs and files as files
    ctrs: List[str] = []
    for root, dirs, files in os.walk(f"{root_dir}"):
        path = root.split(os.sep)
        print((len(path) - 1) * '---', os.path.basename(root))
        ctrs.extend(dirs)
        for file in files:
            pass
            # print(len(path) * '---', file)

    return ctrs

class SHOWAR:
    def __init__(self, running_containers: List[str]) -> None:
        self.running_containers = running_containers
        self.stats_history = {}
        self.sample_rate_ns = 100 * 1e6 # 100ms 
        self.window_len = 10 # 1sec
        self.threshold = 0.15
        
        # State
        self.limits = {}

    def run(self):
        ctr_map = get_ctr_map(self.running_containers)

        for name in self.running_containers:
            self.limits[name] = None
            set_cpu_limit(ctr_map, name, None)

        files = {}
        for name in self.running_containers:
            files[name, 'cpuacct.usage'] = stat_path(ctr_map, name, 'cpuacct.usage').open()
            files[name, 'cpu.stat'] = stat_path(ctr_map, name, 'cpu.stat').open()
            files[name, 'cpu.cfs_quota_us'] = stat_path(ctr_map, name, 'cpu.cfs_quota_us').open()

        monotonic_base = time.time() - time.perf_counter()

        self.stats_history = collections.defaultdict(list)

        late_end_time = 0
        while True:
            print(self.stats_history)
            t = time.perf_counter()
            tt = (0.097 - t) * 1000 % 100 / 1000
            t += tt
            time.sleep(tt)
            
            stats = collections.defaultdict(dict)
            for name in self.running_containers:
                try:
                    files[name, 'cpuacct.usage'].seek(0)
                    stats[name]['cpu_usage'] = files[name, 'cpuacct.usage'].read()
                    files[name, 'cpu.stat'].seek(0)
                    for line in files[name, 'cpu.stat'].read().splitlines():
                        k, v = line.split()
                        stats[name][f'cpu_stat.{k}'] = v
                    files[name, 'cpu.cfs_quota_us'].seek(0)
                    stats[name]['cpu_cfs_quota_us'] = files[name, 'cpu.cfs_quota_us'].read()
                except Exception as e:
                    print(f'At t={t} {name} has exception {e}, skipping ...')

            end_time = time.perf_counter()
            if end_time > t + (-t * 1000 % 100 / 1000):
                late_end_time += 1

            for name in self.running_containers:
                try:
                    stats[name]['cpu_usage'] = int(stats[name]['cpu_usage']) / 1e9
                    stats[name]['dt_cpu_usage'] = (stats[name]['cpu_usage'] - self.stats_history[name][-1][1]['cpu_usage'] if name in self.stats_history else 0)
                    stats[name]['cpu_stat.nr_periods'] = int(stats[name]['cpu_stat.nr_periods'])
                    stats[name]['cpu_stat.nr_throttled'] = int(stats[name]['cpu_stat.nr_throttled'])
                    stats[name]['cpu_stat.throttled_time'] = int(stats[name]['cpu_stat.throttled_time']) / 1e9
                    stats[name]['cpu_stat.throttled_time'] = int(stats[name]['cpu_stat.throttled_time']) / 1e9
                    stats[name]['cpu_cfs_quota_us'] = int(stats[name]['cpu_cfs_quota_us'])
                except Exception as e:
                    print(f'At t={t} {name} error {e}')

            for name in stats:
                self.stats_history[name].append((t + monotonic_base, stats[name]))
                self.stats_history[name] = self.stats_history[name][-self.window_len:]
                self.calc_cpu_mean()

    def calc_cpu_mean(self):
        for name, hist in self.stats_history.items():
            cpu_usages = []
            for (ts, stat) in hist:
                cpu_usages.append(stat['dt_cpu_usage'])
            print(f'Mean: {np.mean(cpu_usages)}, std: {np.std(cpu_usages)}') 

def main():
    running_ctrs: List[str] = []
    while len(running_ctrs) == 0:
        running_ctrs = get_running_containers(f'/sys/fs/cgroup/cpu/docker')
        print(f'Waiting for containers to start ...')
        time.sleep(1)

    showar = SHOWAR(
        running_containers=running_ctrs
    )
    showar.run()

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
import collections
import datetime
import numpy as np
import os
import pathlib
import subprocess
import time
import sys
from typing import Any, Dict, List, Set


def get_ctr_map(namespace, components):
    # ctr_map = {}
    # for name in components:
    #     ctr_map[name] = f"kubepods.slice/{name}/"
    # return ctr_map
    name_to_uid = {}
    name_to_container_ids = {}

    print(f"[DEBUG] Getting pods in namespace: {namespace}")
    # Get pod details including container IDs
    p = subprocess.run([
        'kubectl', 'get', 'pods', f'-n={namespace}', '-o', 'json'
    ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
    import json
    pod_data = json.loads(p.stdout)
    for item in pod_data['items']:
        uid = item['metadata']['uid']
        name_orig = item['metadata']['name']
        name = name_orig.rsplit('-', 2)[0]
        print(f"[DEBUG] Pod: name={name_orig}, parsed={name}, uid={uid}")
        if name in components:
            assert name not in name_to_uid
            name_to_uid[name] = uid
            # Get container IDs from status
            container_ids = []
            statuses = item.get('status', {}).get('containerStatuses', [])
            for status in statuses:
                cid = status.get('containerID', '')
                if cid.startswith('docker://'):
                    cid = cid.replace('docker://', '')
                if cid:
                    container_ids.append(cid)
            name_to_container_ids[name] = container_ids
            print(f"[DEBUG] Container IDs for {name}: {container_ids}")

    uid_to_qos = {}
    cgroup = pathlib.Path('/sys/fs/cgroup')
    found_kubepods = False
    for qos in ['guaranteed', 'burstable', 'besteffort']:
        d = cgroup/f'cpu/kubepods.slice/kubepods-{qos}.slice'
        p = f'kubepods-{qos}-pod'
        s = '.slice'
        print(f"[DEBUG] Checking cgroup dir: {d}")
        if not d.exists():
            print(f"[DEBUG] Cgroup dir {d} does not exist!")
            continue
        found_kubepods = True
        for i in d.glob(f'{p}*{s}'):
            print(f"[DEBUG] Found cgroup: {i}")
            uid = i.name[len(p):-len(s)].replace('_', '-')
            uid_to_qos[uid] = qos

    pod_map = {}
    if found_kubepods:
        for name in components:
            uid = name_to_uid.get(name)
            if not uid:
                print(f"[DEBUG] No UID found for component {name}")
                continue
            try:
                qos = uid_to_qos[uid]
            except KeyError:
                print(f"[DEBUG] No QoS found for UID {uid} (component {name})")
                pass
            else:
                pod_map[name] = qos, uid
    else:
        # Docker fallback: scan for docker-*.scope in system.slice
        docker_cgroup_dir = cgroup / 'cpu/system.slice'
        print(f"[DEBUG] Docker fallback: scanning {docker_cgroup_dir}")
        docker_cgroups = list(docker_cgroup_dir.glob('docker-*.scope'))
        print(f"[DEBUG] All docker cgroups found:")
        for i in docker_cgroups:
            print(f"    {i.name}")
        for i in docker_cgroups:
            fname = i.name
            # docker-<containerid>.scope
            parts = fname.split('-')
            if len(parts) < 2:
                continue
            container_id = parts[1].replace('.scope', '')
            for name, cids in name_to_container_ids.items():
                for cid in cids:
                    if container_id == cid or container_id == cid[:12]:
                        print(f"[DEBUG] Matched docker cgroup {container_id} to pod {name} (container ID: {cid})")
                        pod_map[name] = ('docker', container_id)
        if not pod_map:
            print(f"[DEBUG] No docker containers matched pod container IDs. You may need to adjust matching logic.")
    print(f"[DEBUG] pod_map: {pod_map}")
    return pod_map




def stat_path(ctr_map, name, stat):
    # group = ctr_map[name]
    # return pathlib.Path(f"/sys/fs/cgroup/cpu/{group}/{stat}")

    print(ctr_map)
    qos, uid = ctr_map[name]
    family, _, name = stat.partition('.')
    slices = f'kubepods.slice/kubepods-{qos}.slice/kubepods-{qos}-pod{uid.replace("-", "_")}.slice'
    return pathlib.Path(f'/sys/fs/cgroup/{family}/{slices}/{family}.{name}')


def set_cpu_limit(ctr_map, name, limit, period=0.1):
    period_us = round(period * 1e6)
    assert 1000 <= period_us <= 1000000
    if limit is None:
        quota_us = -1
    else:
        quota_us = round(limit * period_us)
        assert quota_us >= 1000

    stat_path(ctr_map, name, "cpu.cfs_period_us").write_text(str(period_us))
    stat_path(ctr_map, name, "cpu.cfs_quota_us").write_text(str(quota_us))
    print(
        f"{datetime.datetime.now()} Written period={period_us},quota={quota_us} to name={name},(qos,uid)={ctr_map[name]}"
    )

    return


def get_running_containers(root_dir: str):
    # traverse root directory, and list directories as dirs and files as files
    ctrs: List[str] = []
    for root, dirs, files in os.walk(f"{root_dir}"):
        path = root.split(os.sep)
        # print((len(path) - 1) * "---", os.path.basename(root))
        ctrs.extend(dirs)
        for file in files:
            pass
            # print(len(path) * '---', file)

    return ctrs


class SHOWAR:
    def __init__(self, namespace: str, root_dir: str = f"/sys/fs/cgroup/cpu/kubepods.slice/") -> None:
        self.namespace = namespace
        self.running_containers = []
        self.stats_history = {}
        self.ctr_map = {}
        self.root_dir = root_dir
        self.sample_rate_sec = 0.5  # 20ms
        self.scale_freq_sec = 1  # 20ms
        self.last_scale_t = 0
        self.window_len = 10  # 50ms
        self.thresh_perc = 0.15

        # State
        self.spread = {}
        self.last_t = 0
        self.files = {}
        self.components = set({})

        p = subprocess.run(['kubectl', 'get', 'pods', f'-n={namespace}',
        r'-o=jsonpath={range .items[*]}{.metadata.uid} {.metadata.name}{"\n"}{end}'],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
        for i in p.stdout.splitlines():
            uid, name = i.split()
            name = '-'.join(name.split('-')[:-2]) #     name.rsplit('-', 2)[0]
            self.components.add(name)

        self.update_state()
        # Init limits
        for name in self.running_containers:
            self.spread[name] = None
            set_cpu_limit(self.ctr_map, name, None)

    def update_state(self):
        # self.running_containers = get_running_containers(self.root_dir)
        self.ctr_map = get_ctr_map(self.namespace, self.components)
        self.running_containers = self.ctr_map.keys()
        # print(f'run: {self.running_containers}')
        # for name in self.running_containers:
        for name in self.running_containers:
            if name not in self.spread:
                self.spread[name] = None

    def sleep_sample_period(self):
        t = time.perf_counter()
        # tt = (0.097 - t) * 1000 % 100 / 3000  # ~30ms
        tt = self.sample_rate_sec
        print(f'At {t:.4f} sleeping for {tt:.4f} sec ...')
        t += tt
        time.sleep(tt)
        print(f'At {t:.4f} woke up')
        self.last_t = t

    def wait_cgroup_exist(self):
        files_ready = False
        to_check = ["cpuacct.usage", "cpu.stat", "cpu.cfs_quota_us"]
        for name in self.running_containers:
            while not files_ready:
                checked = []
                for f in to_check:
                    stat_obj = stat_path(self.ctr_map, name, f)
                    if stat_obj.is_file():
                        checked.append(f)
                if len(checked) == len(to_check):
                    files_ready = True
                    print(f"Cgroup for {name[:5]} ready! t={self.last_t}")
                else:
                    print(
                        f"Cgroup for {name[:5]} not ready, sleeping ... t={self.last_t}"
                    )
                self.sleep_sample_period()

    def open_cgroup_files(self):
        cgroup_files = [
            "cpuacct.usage",
            "cpu.stat",
            "cpu.cfs_quota_us",
            "cpu.cfs_period_us",
        ]
        for name in self.running_containers:
            for cf in cgroup_files:
                if (name, cf) not in self.files:
                    self.files[name, cf] = stat_path(self.ctr_map, name, cf).open()

    def get_stats(self):
        stats = collections.defaultdict(dict)
        for name in self.running_containers:
            self.files[name, "cpuacct.usage"].seek(0)
            assert self.files[
                name, "cpuacct.usage"
            ], f"{self.files[name, 'cpuacct.usage']} does not exist!"
            stats[name]["cpu_usage"] = self.files[name, "cpuacct.usage"].read()
            self.files[name, "cpu.stat"].seek(0)
            for line in self.files[name, "cpu.stat"].read().splitlines():
                k, v = line.split()
                stats[name][f"cpu_stat.{k}"] = v
            self.files[name, "cpu.cfs_quota_us"].seek(0)
            stats[name]["cpu_cfs_quota_us"] = self.files[
                name, "cpu.cfs_quota_us"
            ].read()
            self.files[name, "cpu.cfs_period_us"].seek(0)
            stats[name]["cpu_cfs_period_us"] = self.files[
                name, "cpu.cfs_period_us"
            ].read()

        return stats

    def run(self):
        monotonic_base = time.time() - time.perf_counter()
        self.stats_history = collections.defaultdict(list)
        while True:
            # Need to parallelize this?
            self.sleep_sample_period()
            self.update_state()
            if len(self.running_containers) == 0:
                print(f"No running containers!")
                self.sleep_sample_period()
                continue

            self.open_cgroup_files()
            stats = self.get_stats()

            # Type + derive values
            for name in self.running_containers:
                try:
                    stats[name]["cpu_usage"] = int(stats[name]["cpu_usage"]) / 1e9
                    stats[name]["dt_cpu_usage"] = (
                        stats[name]["cpu_usage"]
                        - self.stats_history[name][-1][1]["cpu_usage"]
                        if name in self.stats_history
                        else 0
                    )
                    stats[name]["cpu_stat.nr_periods"] = int(
                        stats[name]["cpu_stat.nr_periods"]
                    )
                    stats[name]["cpu_stat.nr_throttled"] = int(
                        stats[name]["cpu_stat.nr_throttled"]
                    )
                    stats[name]["cpu_stat.throttled_time"] = (
                        int(stats[name]["cpu_stat.throttled_time"]) / 1e9
                    )
                    stats[name]["cpu_stat.throttled_time"] = (
                        int(stats[name]["cpu_stat.throttled_time"]) / 1e9
                    )
                    stats[name]["cpu_cfs_quota_us"] = int(
                        stats[name]["cpu_cfs_quota_us"]
                    )
                    stats[name]["cpu_cfs_period_us"] = int(
                        stats[name]["cpu_cfs_period_us"]
                    )
                except Exception as e:
                    print(f"At t={self.last_t} {name} error {e}")

            for name in stats:
                self.stats_history[name].append(
                    (self.last_t + monotonic_base, stats[name])
                )
                self.stats_history[name] = self.stats_history[name][-self.window_len :]

            # SCALE UP/DOWN
            for name in stats:
                cpu_usages = self.get_cpu_usages(name)
                mean = np.mean(cpu_usages)
                std = np.std(cpu_usages)
                spread = mean + (3 * std)
                target_core = spread / self.sample_rate_sec
                print(f'mean={mean:.4f}, std={std:.4f}, Target core for {name[:5]}={target_core:.4f}')
                if not self.spread[name]:
                    self.spread[name] = spread
                else:
                    diff = np.abs(spread - self.spread[name])
                    threshold = self.thresh_perc * self.spread[name]
                    last_scale_diff = np.abs(self.last_scale_t - self.last_t) 
                    print(f"At t={self.last_t:.4f}, {name[:5]}, curr. spread={self.spread[name]:.4f}, obs. spread={spread:.4f}, len={len(self.stats_history[name])}, thresh={threshold:.4f}, last_scale_diff={last_scale_diff:.4f}")
                    # hist = self.stats_history[name][-5:]
                    # dts = []
                    # for e in hist:
                    #     dts.append(e[1]["dt_cpu_usage"])
                    print(f'At t={self.last_t}, dts={cpu_usages}, mu={mean:.4f}, std={std:.4f}')
                    if (diff > threshold) and \
                        (last_scale_diff > self.scale_freq_sec):
                        # limit -> quota_us conversion requires quota >= 1000
                        target_core = max((spread / self.sample_rate_sec), 0.01)
                        set_cpu_limit(self.ctr_map, name, target_core)
                        self.spread[name] = spread
                        self.last_scale_t = self.last_t

    def get_cpu_usages(self, name: str) -> List[float]:
        hist = self.stats_history[name]
        cpu_usages = []
        for ts, stat in hist:
            cpu_usages.append(stat["dt_cpu_usage"])

        return cpu_usages


def main():
    ns=sys.argv[1] if len(sys.argv) > 1 else error("Need namespace arg")
    showar = SHOWAR(namespace=ns)
    showar.run()


if __name__ == "__main__":
    main()

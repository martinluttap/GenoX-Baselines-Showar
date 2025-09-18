
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
def get_container_node_mapping(namespace: str) -> list:
    """
    Returns a list of (node_name, pod_name, container_id) for all containers in the namespace.
    """
    import json
    p = subprocess.run([
        'kubectl', 'get', 'pods', f'-n={namespace}', '-o', 'json'
    ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
    pod_data = json.loads(p.stdout)
    mapping = []
    for item in pod_data['items']:
        node_name = item['spec'].get('nodeName', '')
        pod_name = item['metadata']['name']
        statuses = item.get('status', {}).get('containerStatuses', [])
        for status in statuses:
            cid = status.get('containerID', '')
            if cid.startswith('docker://'):
                cid = cid.replace('docker://', '')
            if cid:
                mapping.append((node_name, pod_name, cid))
    return mapping
def get_docker_container_ids(namespace: str) -> list:
    """
    Returns a list of Docker container IDs for all pods in the given namespace.
    """
    import json
    p = subprocess.run([
        'kubectl', 'get', 'pods', f'-n={namespace}', '-o', 'json'
    ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
    pod_data = json.loads(p.stdout)
    container_ids = []
    for item in pod_data['items']:
        statuses = item.get('status', {}).get('containerStatuses', [])
        for status in statuses:
            cid = status.get('containerID', '')
            if cid.startswith('docker://'):
                cid = cid.replace('docker://', '')
            if cid:
                container_ids.append(cid)
    return container_ids

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
    info = ctr_map[name]
    paths_checked = []
    def exists_and_log(path):
        paths_checked.append(str(path))
        if path.exists():
            print(f"[DEBUG] Found cgroup file: {path}")
            return True
        return False

    cgroupv2_base = pathlib.Path('/sys/fs/cgroup')
    if len(info) == 3:
        # Use pod UID, not pod name
        qos, pod_uid, container_id = info
        pod_uid_str = pod_uid.replace('-', '_')
        # Per-container path
        container_path = cgroupv2_base / f'kubepods.slice/kubepods-pod{pod_uid_str}.slice' / container_id / stat
        if exists_and_log(container_path):
            return container_path
        # Per-pod path
        pod_path = cgroupv2_base / f'kubepods.slice/kubepods-pod{pod_uid_str}.slice' / stat
        if exists_and_log(pod_path):
            return pod_path
    # Fallback to global (should not be used for per-container stats)
    global_path = cgroupv2_base / stat
    if exists_and_log(global_path):
        return global_path
    print(f"[WARNING] Cgroup v2 file missing for {name}: checked {paths_checked}")
    return global_path


def set_cpu_limit(ctr_map, name, limit, period=0.1):
    period_us = round(period * 1e6)
    assert 1000 <= period_us <= 1000000
    cpu_max_path = stat_path(ctr_map, name, "cpu.max")
    if not cpu_max_path.exists():
        print(f"[WARNING] Cgroup file missing for {name}: {cpu_max_path}")
        return
    if limit is None:
        # Remove limit: write "max <period_us>"
        cpu_max_path.write_text(f"max {period_us}")
    else:
        quota_us = round(limit * period_us)
        assert quota_us >= 1000
        cpu_max_path.write_text(f"{quota_us} {period_us}")
    print(
        f"{datetime.datetime.now()} Written cpu.max={cpu_max_path.read_text().strip()} to name={name},(node,pod,container_id)={ctr_map[name]}"
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
        self.pod_map = get_ctr_map(namespace, self.components)

        # Use container-node mapping for discovery
        self.container_node_map = get_container_node_mapping(namespace)
        self.components = set([pod_name.rsplit('-', 2)[0] for _, pod_name, _ in self.container_node_map])
        self.update_state()
        # Init limits
        for name in self.running_containers:
            self.spread[name] = None
            set_cpu_limit(self.ctr_map, name, None)

    def update_state(self):
        # Use container-node mapping for running containers
        self.ctr_map = {}
        for node_name, pod_name, container_id in self.container_node_map:
            name = pod_name.rsplit('-', 2)[0]
            pod_info = self.pod_map.get(name)
            if pod_info:
                qos, pod_uid = pod_info
                self.ctr_map[name] = (qos, pod_uid, container_id)
            else:
                self.ctr_map[name] = (node_name, pod_name, container_id)
        self.running_containers = self.ctr_map.keys()
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
        to_check = ["cpu.stat", "cpu.max"]
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
            "cpu.stat",
            "cpu.max",
        ]
        for name in self.running_containers:
            for cf in cgroup_files:
                if (name, cf) not in self.files:
                    path = stat_path(self.ctr_map, name, cf)
                    if not path.exists():
                        print(f"[WARNING] Cgroup v2 file missing for {name}: {path}")
                        continue
                    self.files[name, cf] = path.open()

    def get_stats(self):
        stats = collections.defaultdict(dict)
        for name in self.running_containers:
            missing = False
            for cf in ["cpu.stat", "cpu.max"]:
                if (name, cf) not in self.files:
                    print(f"[WARNING] Skipping {name}: missing {cf}")
                    missing = True
                    break
            if missing:
                continue
            self.files[name, "cpu.stat"].seek(0)
            for line in self.files[name, "cpu.stat"].read().splitlines():
                k, v = line.split()
                stats[name][f"cpu_stat.{k}"] = v
            # cpu usage in microseconds (cgroup v2: usage_usec)
            stats[name]["cpu_usage"] = int(stats[name].get("cpu_stat.usage_usec", 0)) / 1e6
            self.files[name, "cpu.max"].seek(0)
            cpu_max = self.files[name, "cpu.max"].read().strip().split()
            stats[name]["cpu_max_quota_us"] = cpu_max[0] if len(cpu_max) > 0 else "max"
            stats[name]["cpu_max_period_us"] = cpu_max[1] if len(cpu_max) > 1 else "100000"
        return stats

    def run(self):
        monotonic_base = time.time() - time.perf_counter()
        self.stats_history = collections.defaultdict(list)
        while True:
            self.sleep_sample_period()
            self.update_state()
            if len(self.running_containers) == 0:
                print(f"No running containers!")
                self.sleep_sample_period()
                continue

            self.open_cgroup_files()
            stats = self.get_stats()

            # Only process containers with valid stats
            valid_names = [name for name in self.running_containers if name in stats]

            # Type + derive values
            for name in valid_names:
                try:
                    # cpu_usage already in ms
                    stats[name]["dt_cpu_usage"] = (
                        stats[name]["cpu_usage"]
                        - self.stats_history[name][-1][1]["cpu_usage"]
                        if name in self.stats_history and self.stats_history[name]
                        else 0
                    )
                    stats[name]["cpu_stat.nr_periods"] = int(stats[name]["cpu_stat.nr_periods"])
                    stats[name]["cpu_stat.nr_throttled"] = int(stats[name]["cpu_stat.nr_throttled"])
                    stats[name]["cpu_stat.throttled_time"] = int(stats[name]["cpu_stat.throttled_time"]) / 1e6
                    stats[name]["cpu_max_quota_us"] = int(stats[name]["cpu_max_quota_us"]) if stats[name]["cpu_max_quota_us"] != "max" else -1
                    stats[name]["cpu_max_period_us"] = int(stats[name]["cpu_max_period_us"])
                except Exception as e:
                    print(f"At t={self.last_t} {name} error {e}")

            for name in valid_names:
                self.stats_history[name].append((self.last_t + monotonic_base, stats[name]))
                self.stats_history[name] = self.stats_history[name][-self.window_len :]

            # SCALE UP/DOWN
            for name in valid_names:
                cpu_usages = self.get_cpu_usages(name)
                if not cpu_usages:
                    print(f"[WARNING] No cpu_usages for {name}, skipping scaling.")
                    continue
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
                    print(f'At t={self.last_t}, dts={cpu_usages}, mu={mean:.4f}, std={std:.4f}')
                    if (diff > threshold) and (last_scale_diff > self.scale_freq_sec):
                        target_core = max((spread / self.sample_rate_sec), 0.01)
                        set_cpu_limit(self.ctr_map, name, target_core)
                        self.spread[name] = spread
                        self.last_scale_t = self.last_t

    def get_cpu_usages(self, name: str) -> List[float]:
        hist = self.stats_history.get(name, [])
        cpu_usages = []
        for ts, stat in hist:
            if "dt_cpu_usage" in stat:
                cpu_usages.append(stat["dt_cpu_usage"])
        return cpu_usages


def main():
    ns=sys.argv[1] if len(sys.argv) > 1 else error("Need namespace arg")
    showar = SHOWAR(namespace=ns)
    showar.run()


if __name__ == "__main__":
    main()

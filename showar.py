def find_container_cgroups():
    """
    Return a dict mapping container name to cgroup path for all containers on the node.
    """
    import pathlib
    cgroupv2_base = pathlib.Path('/sys/fs/cgroup')
    containers = {}
    for scope in cgroupv2_base.glob('**/*.scope'):
        name = scope.name
        containers[name] = scope
    return containers
def print_all_container_cpu_max():
    """
    Print cpu.max for all containers running on the current node (cgroup v2).
    """
    import pathlib
    cgroupv2_base = pathlib.Path('/sys/fs/cgroup')
    # Find all docker and containerd scope directories
    for scope in cgroupv2_base.glob('**/*.scope'):
        cpu_max_path = scope / 'cpu.max'
        if cpu_max_path.exists():
            print(f"{scope.name}: {cpu_max_path.read_text().strip()}")
        else:
            print(f"{scope.name}: cpu.max not found")

#!/usr/bin/env python3
import collections
import datetime
import numpy as np
import os
import pathlib
import subprocess
import time
import sys
import logging
from typing import Any, Dict, List, Set
logging.basicConfig(
    filename='showar.log',
    filemode='a',
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.DEBUG
)
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

    logging.debug(f"Getting pods in namespace: {namespace}")
    # Get pod details including container IDs
    p = subprocess.run([
        'kubectl', 'get', 'pods', f'-n={namespace}', '-o', 'json'
    ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
    import json
    pod_data = json.loads(p.stdout)
    for item in pod_data['items']:
        pod_uid = item['metadata']['uid']  # Always use real pod UID
        name_orig = item['metadata']['name']
        name = name_orig.rsplit('-', 2)[0]
        logging.debug(f"Pod: name={name_orig}, parsed={name}, pod_uid={pod_uid}")
        if name in components:
            assert name not in name_to_uid
            name_to_uid[name] = pod_uid  # Store real pod UID
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
            logging.debug(f"Container IDs for {name}: {container_ids}")

    uid_to_qos = {}
    cgroup = pathlib.Path('/sys/fs/cgroup')
    found_kubepods = False
    for qos in ['guaranteed', 'burstable', 'besteffort']:
        d = cgroup/f'cpu/kubepods.slice/kubepods-{qos}.slice'
        p = f'kubepods-{qos}-pod'
        s = '.slice'
        logging.debug(f"Checking cgroup dir: {d}")
        if not d.exists():
            logging.info(f"[DEBUG] Cgroup dir {d} does not exist!")
            continue
        found_kubepods = True
        for i in d.glob(f'{p}*{s}'):
            logging.debug(f"Found cgroup: {i}")
            uid = i.name[len(p):-len(s)].replace('_', '-')
            uid_to_qos[uid] = qos

    pod_map = {}
    if found_kubepods:
        for name in components:
            uid = name_to_uid.get(name)
            if not uid:
                logging.debug(f"No UID found for component {name}")
                continue
            try:
                qos = uid_to_qos[uid]
            except KeyError:
                logging.debug(f"No QoS found for UID {uid} (component {name})")
                pass
            else:
                pod_map[name] = qos, uid
    else:
        # Docker fallback: scan for docker-*.scope in system.slice
        docker_cgroup_dir = cgroup / 'cpu/system.slice'
        logging.debug(f"Docker fallback: scanning {docker_cgroup_dir}")
        docker_cgroups = list(docker_cgroup_dir.glob('docker-*.scope'))
        logging.debug(f"All docker cgroups found:")
        for i in docker_cgroups:
            logging.debug(f"    {i.name}")
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
                        logging.debug(f"Matched docker cgroup {container_id} to pod {name} (container ID: {cid})")
                        pod_map[name] = ('docker', container_id)
        if not pod_map:
            logging.debug(f"No docker containers matched pod container IDs. You may need to adjust matching logic.")
    logging.debug(f"pod_map: {pod_map}")
    return pod_map




def stat_path(ctr_map, name, stat):
    info = ctr_map[name]
    paths_checked = []
    def exists_and_log(path):
        paths_checked.append(str(path))
        if path.exists():
            logging.info(f"[DEBUG] Found cgroup file: {path}")
            return True
        return False

    import pathlib
    cgroupv2_base = pathlib.Path('/sys/fs/cgroup')
    # If info is a PosixPath, just return info/stat
    if isinstance(info, pathlib.Path):
        file_path = info / stat
        exists_and_log(file_path)
        return file_path
    # ...existing code for tuple logic (if needed in future)...
    if len(info) == 3:
        qos, pod_uid, container_id = info
        pod_uid_str = pod_uid.replace('-', '_')
        pod_slice = cgroupv2_base / f'kubepods-pod{pod_uid_str}.slice'
        containerd_scope = pod_slice / f'cri-containerd-{container_id}.scope' / stat
        if exists_and_log(containerd_scope):
            return containerd_scope
        docker_scope = pod_slice / f'docker-{container_id}.scope' / stat
        if exists_and_log(docker_scope):
            return docker_scope
        pod_path = pod_slice / stat
        if exists_and_log(pod_path):
            return pod_path
    global_path = cgroupv2_base / stat
    if exists_and_log(global_path):
        return global_path
    logging.info(f"[WARNING] Cgroup v2 file missing for {name}: checked {paths_checked}")
    return global_path


def set_cpu_limit(ctr_map, name, limit, period=0.1):
    period_us = round(period * 1e6)
    assert 1000 <= period_us <= 1000000
    cpu_max_path = stat_path(ctr_map, name, "cpu.max")
    if not cpu_max_path.exists():
        logging.info(f"[WARNING] Cgroup file missing for {name}: {cpu_max_path}")
        return
    if limit is None:
        # Remove limit: write "max <period_us>"
        cpu_max_path.write_text(f"max {period_us}")
    else:
        quota_us = round(limit * period_us)
        assert quota_us >= 1000
        cpu_max_path.write_text(f"{quota_us} {period_us}")
    logging.info(
        f"{datetime.datetime.now()} Written cpu.max={cpu_max_path.read_text().strip()} to name={name},(node,pod,container_id)={ctr_map[name]}"
    )
    return


def get_running_containers(root_dir: str):
    # traverse root directory, and list directories as dirs and files as files
    ctrs: List[str] = []
    for root, dirs, files in os.walk(f"{root_dir}"):
        path = root.split(os.sep)
        # logging.info((len(path) - 1) * "---", os.path.basename(root))
        ctrs.extend(dirs)
        for file in files:
            pass
            # logging.info(len(path) * '---', file)

    return ctrs


class SHOWAR:
    def __init__(self):
        self.running_containers = []
        self.stats_history = {}
        self.ctr_map = {}
        self.sample_rate_sec = 0.5  # 20ms
        self.scale_freq_sec = 1  # 20ms
        self.last_scale_t = 0
        self.window_len = 10  # 50ms
        self.thresh_perc = 0.15

        # State
        self.spread = {}
        self.last_t = 0
        self.files = {}

        # Discover containers on node
        self.ctr_map = find_container_cgroups()
        self.running_containers = list(self.ctr_map.keys())
        for name in self.running_containers:
            self.spread[name] = None

    def update_state(self):
        # Refresh container cgroups
        self.ctr_map = find_container_cgroups()
        self.running_containers = list(self.ctr_map.keys())
        for name in self.running_containers:
            if name not in self.spread:
                self.spread[name] = None

    def sleep_sample_period(self):
        t = time.perf_counter()
        # tt = (0.097 - t) * 1000 % 100 / 3000  # ~30ms
        tt = self.sample_rate_sec
        logging.info(f'At {t:.4f} sleeping for {tt:.4f} sec ...')
        t += tt
        time.sleep(tt)
        logging.info(f'At {t:.4f} woke up')
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
                    logging.info(f"Cgroup for {name[:5]} ready! t={self.last_t}")
                else:
                    logging.info(
                        f"Cgroup for {name[:5]} not ready, sleeping ... t={self.last_t}"
                    )
                self.sleep_sample_period()

    def open_cgroup_files(self):
        cgroup_files = ["cpu.stat", "cpu.max"]
        for name in self.running_containers:
            cgroup_path = self.ctr_map[name]
            for cf in cgroup_files:
                file_path = cgroup_path / cf
                if (name, cf) not in self.files:
                    if not file_path.exists():
                        logging.info(f"[WARNING] Cgroup v2 file missing for {name}: {file_path}")
                        continue
                    self.files[name, cf] = file_path.open()

    def get_stats(self):
        stats = collections.defaultdict(dict)
        for name in self.running_containers:
            missing = False
            for cf in ["cpu.stat", "cpu.max"]:
                if (name, cf) not in self.files:
                    logging.info(f"[WARNING] Skipping {name}: missing {cf}")
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
                logging.info(f"No running containers!")
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
                    stats[name]["cpu_stat.nr_periods"] = int(stats[name].get("cpu_stat.nr_periods", 0))
                    stats[name]["cpu_stat.nr_throttled"] = int(stats[name].get("cpu_stat.nr_throttled", 0))
                    stats[name]["cpu_stat.throttled_time"] = int(stats[name].get("cpu_stat.throttled_time", 0)) / 1e6
                    stats[name]["cpu_max_quota_us"] = int(stats[name]["cpu_max_quota_us"]) if stats[name]["cpu_max_quota_us"] != "max" else -1
                    stats[name]["cpu_max_period_us"] = int(stats[name]["cpu_max_period_us"])
                except Exception as e:
                    logging.info(f"At t={self.last_t} {name} error {e}")

            for name in valid_names:
                self.stats_history[name].append((self.last_t + monotonic_base, stats[name]))
                self.stats_history[name] = self.stats_history[name][-self.window_len :]

            # SCALE UP/DOWN
            for name in valid_names:
                cpu_usages = self.get_cpu_usages(name)
                if not cpu_usages:
                    logging.info(f"[WARNING] No cpu_usages for {name}, skipping scaling.")
                    continue
                mean = np.mean(cpu_usages)
                std = np.std(cpu_usages)
                spread = mean + (3 * std)
                target_core = spread / self.sample_rate_sec
                logging.info(f'mean={mean:.4f}, std={std:.4f}, Target core for {name[:5]}={target_core:.4f}')
                if not self.spread[name]:
                    self.spread[name] = spread
                else:
                    diff = np.abs(spread - self.spread[name])
                    threshold = self.thresh_perc * self.spread[name]
                    last_scale_diff = np.abs(self.last_scale_t - self.last_t)
                    logging.info(f"At t={self.last_t:.4f}, {name[:5]}, curr. spread={self.spread[name]:.4f}, obs. spread={spread:.4f}, len={len(self.stats_history[name])}, thresh={threshold:.4f}, last_scale_diff={last_scale_diff:.4f}")
                    logging.info(f'At t={self.last_t}, dts={cpu_usages}, mu={mean:.4f}, std={std:.4f}')
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
    showar = SHOWAR()
    showar.run()


if __name__ == "__main__":
    main()

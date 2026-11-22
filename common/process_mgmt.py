"""
process_mgmt.py
----------------
[MILESTONE 10 - Naming and Distributed Process Management]

A distributed PROCESS (a long-running service instance such as a "session handler")
goes through five stages, which are exactly the five in the brief:

    CREATE      describe it and give it a global NAME            (/proc/session-7)
    SCHEDULE    decide WHICH node it should run on
    START       run it there; the name is REGISTERED in the naming service
    LOCATE      later, anyone finds it by NAME -- through name resolution, never by
                knowing the node (this is where Milestone 9's location transparency
                and this milestone's naming subsystem meet)
    COORDINATE  start/stop groups of processes together, move them, and respawn them
                elsewhere when their node dies
    TERMINATE   stop it, release its resources, remove its name

Reuse of earlier milestones:
  * group start is ATOMIC (all processes start, or none) using the Milestone 5
    Two-Phase Commit;
  * "location-aware" placement is the Milestone 8 scoring idea (distance + a load penalty);
  * automatic respawn is triggered by the Milestone 7 failure detector.
"""

import threading
import time

from common.communication import send_message
from common.system_models import SIM_RTT_MS, TIER_OF, LOAD_PENALTY_MS
from common.transaction import TwoPhaseCommitCoordinator

POLICIES = ("round_robin", "least_loaded", "location_aware")


class Scheduler:
    """Decides which node runs a process, keeping its own account of what it already placed."""

    def __init__(self, cluster, policy="location_aware"):
        assert policy in POLICIES
        self.cluster, self.policy = cluster, policy
        self.nodes = list(cluster.configs)
        self.caps = {n: (c.cpu_capacity, c.memory_capacity, c.bandwidth_capacity) for n, c in cluster.configs.items()}
        self.inflight = {n: [0.0, 0.0, 0.0] for n in self.nodes}
        self.dead = set()
        self._rr = 0
        self._lock = threading.Lock()

    def _fits(self, n, spec):
        cap, inf = self.caps[n], self.inflight[n]
        return (inf[0] + spec["cpu"] <= cap[0] and inf[1] + spec["memory"] <= cap[1]
                and inf[2] + spec["bandwidth"] <= cap[2])

    def _order(self, spec):
        if self.policy == "round_robin":
            k = self._rr % len(self.nodes)
            self._rr += 1
            return self.nodes[k:] + self.nodes[:k]
        if self.policy == "least_loaded":
            return sorted(self.nodes, key=lambda n: self.inflight[n][0] / self.caps[n][0])

        def score(n):
            tier = TIER_OF[n]
            s = SIM_RTT_MS[tier] + LOAD_PENALTY_MS * self.inflight[n][0] / self.caps[n][0]
            if spec.get("service_class") == "URLLC" and tier in ("core", "cloud"):
                s += 1000
            return s
        return sorted(self.nodes, key=score)

    def place(self, spec):
        with self._lock:
            for n in self._order(spec):
                if n not in self.dead and self._fits(n, spec):
                    inf = self.inflight[n]
                    inf[0] += spec["cpu"]; inf[1] += spec["memory"]; inf[2] += spec["bandwidth"]
                    return n
        return None

    def release(self, node, spec):
        with self._lock:
            inf = self.inflight[node]
            inf[0] -= spec["cpu"]; inf[1] -= spec["memory"]; inf[2] -= spec["bandwidth"]

    def mark_dead(self, node):
        with self._lock:
            self.dead.add(node)
            self.inflight[node] = [0.0, 0.0, 0.0]      # whatever ran there is gone

    def revive(self, node):
        with self._lock:
            self.dead.discard(node)

    def utilization(self):
        with self._lock:
            return {n: self.inflight[n][0] / self.caps[n][0] for n in self.nodes}


def _ms(t0):
    return (time.perf_counter() - t0) * 1000.0


class DistributedProcessManager:
    def __init__(self, cluster, resolver, scheduler, state_transfer_s=0.05):
        self.cluster, self.resolver, self.scheduler = cluster, resolver, scheduler
        self.state_transfer_s = state_transfer_s     # time to hand a process's state over when it moves (simulated)
        self.procs = {}                              # name -> {"spec", "node", "group"}
        self.groups = {}
        self._lock = threading.Lock()

    def _send(self, node, msg, timeout=3.0):
        return send_message(*self.cluster.addr(node), msg, timeout=timeout)

    # ---- single process lifecycle ------------------------------------------
    def start(self, name, spec):
        t0 = time.perf_counter()
        rec = {"name": name, "spec": dict(spec)}                         # CREATE
        t1 = time.perf_counter()
        node = self.scheduler.place(rec["spec"])                         # SCHEDULE
        t2 = time.perf_counter()
        if node is None:
            return {"status": "rejected", "reason": "no_capacity"}
        try:
            r = self._send(node, {"type": "proc_start", "name": name, "spec": rec["spec"]})   # START
        except Exception:
            self.scheduler.release(node, spec)
            self.scheduler.mark_dead(node)
            return {"status": "failed", "reason": "node_unreachable", "node": node}
        t3 = time.perf_counter()
        if r.get("status") != "started":
            self.scheduler.release(node, spec)
            return {"status": "rejected", "reason": r.get("reason", "refused"), "node": node}
        self.resolver.register(name, node)                               # REGISTER the name
        t4 = time.perf_counter()
        with self._lock:
            self.procs[name] = {"spec": rec["spec"], "node": node, "group": None}
        return {"status": "started", "node": node,
                "stages_ms": {"create": (t1 - t0) * 1000, "schedule": (t2 - t1) * 1000,
                              "start": (t3 - t2) * 1000, "register": (t4 - t3) * 1000}}

    def locate(self, name):
        t0 = time.perf_counter()
        node = self.resolver.resolve(name)
        return node, _ms(t0)

    def terminate(self, name):
        node, loc_ms = self.locate(name)                                  # LOCATE by name, then act
        if node is None:
            return {"status": "not_found"}
        t0 = time.perf_counter()
        r = self._send(node, {"type": "proc_terminate", "name": name})
        self.resolver.unregister(name)
        with self._lock:
            rec = self.procs.pop(name, None)
        if rec:
            self.scheduler.release(rec["node"], rec["spec"])
        return {"status": r.get("status"), "stages_ms": {"locate": loc_ms, "terminate": _ms(t0)}}

    # ---- COORDINATE: atomic group start, ordered stop -----------------------
    def start_group(self, group, members):
        """members: [(name, spec), ...] in dependency order. All start, or none does (Two-Phase Commit)."""
        placements = []
        for name, spec in members:
            node = self.scheduler.place(spec)
            if node is None:
                for _, s, n in placements:
                    self.scheduler.release(n, s)
                return {"status": "aborted", "reason": "no_capacity_when_scheduling"}
            placements.append((name, spec, node))
        per_node = {}
        for _, spec, node in placements:
            r = per_node.setdefault(node, {"cpu": 0.0, "memory": 0.0, "bandwidth": 0.0})
            r["cpu"] += spec["cpu"]; r["memory"] += spec["memory"]; r["bandwidth"] += spec["bandwidth"]
        participants = {n: {"host": self.cluster.addr(n)[0], "port": self.cluster.addr(n)[1], "reservation": res}
                        for n, res in per_node.items()}
        out = TwoPhaseCommitCoordinator(participants, send_message, prepare_timeout=1.5).run()
        if out["decision"] != "commit":
            for _, s, n in placements:
                self.scheduler.release(n, s)
            return {"status": "aborted", "reason": "a_node_voted_abort", "votes": out["votes"],
                    "twopc_ms": out["elapsed_ms"]}
        for name, spec, node in placements:
            self._send(node, {"type": "proc_start", "name": name, "spec": spec, "reserved_tx": out["tx_id"]})
            self.resolver.register(name, node)
            with self._lock:
                self.procs[name] = {"spec": spec, "node": node, "group": group}
        self.groups[group] = {"tx_id": out["tx_id"], "members": [m[0] for m in members], "nodes": list(per_node)}
        return {"status": "started", "tx_id": out["tx_id"], "nodes": list(per_node), "twopc_ms": out["elapsed_ms"],
                "placements": {name: node for name, _, node in placements}}

    def terminate_group(self, group):
        g = self.groups.pop(group)
        for name in reversed(g["members"]):                               # stop in reverse dependency order
            self.terminate(name)
        for node in g["nodes"]:
            try:
                self._send(node, {"type": "tx_release", "tx_id": g["tx_id"]})
            except Exception:
                pass
        return {"status": "terminated", "members": len(g["members"])}

    # ---- COORDINATE: move a running process ---------------------------------
    def migrate(self, name, dst, make_before_break=True):
        rec = self.procs[name]
        src, spec = rec["node"], rec["spec"]
        t0 = time.time()
        if make_before_break:
            self.scheduler.inflight[dst][0] += spec["cpu"]; self.scheduler.inflight[dst][1] += spec["memory"]
            self.scheduler.inflight[dst][2] += spec["bandwidth"]
            self._send(dst, {"type": "proc_start", "name": name, "spec": spec})   # new copy first ...
            time.sleep(self.state_transfer_s)                                      # ... while the old one keeps serving
            self.resolver.register(name, dst)                                      # switch the name
            self._send(src, {"type": "proc_terminate", "name": name})              # then retire the old copy
        else:
            self._send(src, {"type": "proc_terminate", "name": name})              # stop first ...
            self.resolver.unregister(name)
            time.sleep(self.state_transfer_s)                                      # ... the service is DOWN during the hand-over
            self.scheduler.inflight[dst][0] += spec["cpu"]; self.scheduler.inflight[dst][1] += spec["memory"]
            self.scheduler.inflight[dst][2] += spec["bandwidth"]
            self._send(dst, {"type": "proc_start", "name": name, "spec": spec})
            self.resolver.register(name, dst)
        self.scheduler.release(src, spec)
        with self._lock:
            rec["node"] = dst
        return {"seconds": time.time() - t0, "from": src, "to": dst}

    # ---- COORDINATE: recover from a node crash ------------------------------
    def respawn_lost(self, crashed):
        lost = [n for n, r in list(self.procs.items()) if r["node"] == crashed and r["group"] is None]
        self.scheduler.mark_dead(crashed)
        restored, failed = [], []
        for name in lost:
            spec = self.procs[name]["spec"]
            node = self.scheduler.place(spec)
            if node is None:
                failed.append(name)
                continue
            try:
                r = self._send(node, {"type": "proc_start", "name": name, "spec": spec})
            except Exception:
                self.scheduler.release(node, spec)
                failed.append(name)
                continue
            if r.get("status") == "started":
                self.resolver.register(name, node)      # the name now points at the NEW location
                self.procs[name]["node"] = node
                restored.append(name)
            else:
                self.scheduler.release(node, spec)
                failed.append(name)
        return {"lost": len(lost), "restored": len(restored), "failed": len(failed)}

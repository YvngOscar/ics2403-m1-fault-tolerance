"""
fault_tolerance.py
-------------------
[MILESTONE 7 - Fault Tolerance and Recovery]

Everything needed to break the platform on purpose and make it cope:

  FaultInjector    - causes a crash, omission, communication (delay),
                      service, or Byzantine failure on a chosen node.
  FailureDetector  - heartbeat monitoring: pings every node every
                      100 ms; 3 missed heartbeats in a row => "suspected".
                      It also CLASSIFIES what it saw (crash vs timeout
                      vs service failure).
  ReplicatedClient - how a client talks to a service that is replicated
                      on several nodes: "single" (no fault tolerance),
                      "failover" (try the next replica), or "quorum"
                      (ask all replicas, trust the majority -- this is
                      what defeats a Byzantine node).
  Supervisor       - the RECOVERY mechanism: restarts a crashed node,
                      re-syncs it, and re-elects the leader with the
                      Bully algorithm (Milestone 4) if the leader died.
  run_open_loop    - sends requests at a FIXED rate regardless of how
                      slow earlier ones are, so a failure cannot hide
                      by simply slowing the test down.

Availability (from the brief):   Availability = T_uptime / T_total
MTTR          (from the brief):   Mean Time To Recovery
"""

import queue
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common.communication import send_message
from common.coordination import BullyElection
from common.integrity import task_checksum

FAULT_KINDS = ("crash", "omission", "delay", "service_down", "byzantine")


# =============================================================================
# Fault injection
# =============================================================================

class FaultInjector:
    def __init__(self, cluster):
        self.cluster = cluster

    def inject(self, node: str, kind: str, **params):
        if kind == "crash":
            self.cluster.kill(node)                       # the OS process really dies
        elif kind in FAULT_KINDS:
            host, port = self.cluster.addr(node)
            send_message(host, port, {"type": "fault_inject", "mode": kind, "params": params}, timeout=2.0)
        else:
            raise ValueError(f"unknown fault kind {kind}")

    def clear(self, node: str, kind: str):
        """Ends a TRANSIENT fault. (A crash is not 'cleared' -- it needs a restart.)"""
        if kind == "crash":
            return
        host, port = self.cluster.addr(node)
        send_message(host, port, {"type": "fault_clear"}, timeout=2.0)


# =============================================================================
# Failure detection (heartbeats)
# =============================================================================

class FailureDetector:
    def __init__(self, cluster, interval=0.1, probe_timeout=0.15, misses_to_suspect=3,
                 on_failure=None, on_recovery=None):
        self.cluster = cluster
        self.interval = interval
        self.probe_timeout = probe_timeout
        self.misses_to_suspect = misses_to_suspect
        self.on_failure = on_failure
        self.on_recovery = on_recovery
        self.state = {n: {"suspected": False, "misses": 0, "kind": None,
                          "suspected_at": None, "recovered_at": None} for n in cluster.configs}
        self.events = []
        self._running = False
        self._threads = []

    def _probe(self, name):
        """Returns None if healthy, else a string classifying the problem."""
        host, port = self.cluster.addr(name)
        try:
            send_message(host, port, {"type": "ping"}, timeout=self.probe_timeout)
        except (socket.timeout, TimeoutError):
            return "timeout"            # alive-looking but not answering in time (omission / slow link)
        except Exception:
            return "crash"              # connection refused / reset: the process is gone
        try:
            r = send_message(host, port, {"type": "service_health"}, timeout=self.probe_timeout)
            if r.get("status") != "ok":
                return "service_failure"  # node answers pings, but its SERVICE is broken
        except (socket.timeout, TimeoutError):
            return "timeout"
        except Exception:
            return "crash"
        return None

    def _monitor(self, name):
        st = self.state[name]
        while self._running:
            problem = self._probe(name)
            now = time.time()
            if problem:
                st["misses"] += 1
                st["kind"] = problem
                if st["misses"] >= self.misses_to_suspect and not st["suspected"]:
                    st["suspected"] = True
                    st["suspected_at"] = now
                    self.events.append((now, name, "suspected", problem))
                    if self.on_failure:
                        try:
                            self.on_failure(name, problem)
                        except Exception as e:  # a recovery bug must never kill the monitor
                            self.events.append((time.time(), name, "callback_error", str(e)))
            else:
                if st["suspected"]:
                    st["suspected"] = False
                    st["recovered_at"] = now
                    self.events.append((now, name, "recovered", None))
                    if self.on_recovery:
                        try:
                            self.on_recovery(name)
                        except Exception as e:
                            self.events.append((time.time(), name, "callback_error", str(e)))
                st["misses"] = 0
            time.sleep(self.interval)

    def start(self):
        self._running = True
        for n in self.cluster.configs:
            t = threading.Thread(target=self._monitor, args=(n,), daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._running = False
        for t in self._threads:
            t.join(timeout=1.5)

    def is_suspected(self, name) -> bool:
        return self.state[name]["suspected"]


# =============================================================================
# Replication: how a client uses a replicated service
# =============================================================================

class ReplicatedClient:
    """
    mode="single"   : talk to replicas[0] only. No fault tolerance (the baseline).
    mode="failover" : try replicas in order, skipping ones the detector suspects;
                      on a timeout / error / service failure, move to the next.
    mode="quorum"   : ask ALL replicas, accept the answer a MAJORITY agrees on.
    """

    def __init__(self, cluster, replicas, detector=None, mode="single", timeout=0.25):
        self.cluster = cluster
        self.replicas = list(replicas)
        self.detector = detector
        self.mode = mode
        self.timeout = timeout

    def _ask(self, node, task):
        host, port = self.cluster.addr(node)
        return send_message(host, port, {"type": "task_request", "task": task}, timeout=self.timeout)

    def request(self, task) -> dict:
        t0 = time.time()
        expected = task_checksum(task)

        if self.mode == "single":
            node = self.replicas[0]
            try:
                r = self._ask(node, task)
                ok = r.get("status") == "completed"
                return self._pack(t0, ok, node, r.get("result"), expected, attempts=1)
            except Exception:
                return self._pack(t0, False, node, None, expected, attempts=1)

        if self.mode == "failover":
            order = [n for n in self.replicas if not (self.detector and self.detector.is_suspected(n))]
            order = order or list(self.replicas)  # if everyone is suspected, try anyway
            attempts = 0
            for node in order:
                attempts += 1
                try:
                    r = self._ask(node, task)
                    if r.get("status") == "completed":
                        return self._pack(t0, True, node, r.get("result"), expected, attempts=attempts)
                except Exception:
                    pass
            return self._pack(t0, False, None, None, expected, attempts=attempts)

        if self.mode == "quorum":
            return self._quorum(task, t0, expected)

        raise ValueError(self.mode)

    def _quorum(self, task, t0, expected):
        need = len(self.replicas) // 2 + 1
        q = queue.Queue()

        def worker(node):
            try:
                r = self._ask(node, task)
                q.put((node, r.get("result") if r.get("status") == "completed" else None))
            except Exception:
                q.put((node, None))

        for n in self.replicas:
            threading.Thread(target=worker, args=(n,), daemon=True).start()

        answers = {}
        deadline = time.time() + self.timeout + 0.1
        winner, tally = None, {}
        while len(answers) < len(self.replicas) and time.time() < deadline:
            try:
                node, val = q.get(timeout=max(0.01, deadline - time.time()))
            except queue.Empty:
                break
            answers[node] = val
            if val is not None:
                tally[val] = tally.get(val, 0) + 1
                if tally[val] >= need:
                    winner = val
                    break
        # let any still-in-flight replicas finish quickly so we can spot dissenters
        end = time.time() + 0.05
        while len(answers) < len(self.replicas) and time.time() < end:
            try:
                node, val = q.get(timeout=0.01)
                answers[node] = val
            except queue.Empty:
                pass
        dissenters = [n for n, v in answers.items() if v is not None and winner is not None and v != winner]
        out = self._pack(t0, winner is not None, None, winner, expected, attempts=len(self.replicas))
        out["dissenters"] = dissenters
        return out

    @staticmethod
    def _pack(t0, ok, node, result, expected, attempts):
        return {"ok": ok, "node": node, "result": result,
                "correct": (ok and result == expected),
                "latency_ms": round((time.time() - t0) * 1000.0, 3),
                "attempts": attempts}


# =============================================================================
# Leader re-election (Bully, Milestone 4) used by the Supervisor
# =============================================================================

def run_bully_among(directory: dict, initiator: str) -> dict:
    """
    Runs the Bully algorithm among the nodes in `directory` (only the nodes
    believed ALIVE), starting from `initiator`, following the chain of
    higher-ranked nodes exactly as milestone4_coordination.py does.
    """
    t0 = time.time()
    total_messages, rounds = 0, 0
    current = initiator
    while True:
        peers = {n: p for n, p in directory.items() if n != current}
        me = directory[current]
        el = BullyElection(current, me["election_id"], peers, send_message,
                           self_host=me["host"], self_port=me["port"])
        out = el.run_election(timeout=0.5)
        total_messages += out["messages_sent"]
        rounds += 1
        if out["declared_leader"] is not None:
            return {"leader": out["declared_leader"], "rounds": rounds,
                    "messages": total_messages, "elapsed_ms": round((time.time() - t0) * 1000.0, 3)}
        higher = el._higher_peers()
        current = min(higher.items(), key=lambda kv: kv[1]["election_id"])[0]


# =============================================================================
# Recovery: the Supervisor
# =============================================================================

class Supervisor:
    """
    Reacts to the FailureDetector:
      * crash            -> (if the dead node was the leader) re-elect, then RESTART the node
      * node comes back  -> RESYNC it (tell it who the leader is); if it outranks the
                            current leader, it takes over (Bully rule).
    Every action is timestamped into `self.events` for the recovery-time analysis.
    """

    def __init__(self, cluster, auto_restart=True):
        self.cluster = cluster
        self.auto_restart = auto_restart
        self.detector = None  # set after the detector is constructed
        self.leader = None
        self.events = []

    def _rec(self, event, **detail):
        self.events.append({"t": time.time(), "event": event, **detail})

    def _alive(self, extra=None):
        names = [n for n in self.cluster.configs
                 if (extra and n in extra) or not self.detector.is_suspected(n)]
        return names

    def set_leader(self, name):
        self.leader = name

    def elect(self, initiator=None, among=None):
        alive = among or self._alive()
        directory = self.cluster.directory(only=alive)
        init = initiator or min(alive, key=lambda n: self.cluster.configs[n].election_id)
        result = run_bully_among(directory, init)
        self.leader = result["leader"]
        # consistency check: do all alive nodes agree?
        views = {}
        for n in alive:
            try:
                h, p = self.cluster.addr(n)
                views[n] = send_message(h, p, {"type": "leader_query"}, timeout=0.5).get("current_leader")
            except Exception:
                views[n] = None
        result["consistent"] = len(set(views.values())) == 1 and self.leader in views.values()
        self._rec("elected", **result)
        return result

    # ---- detector callbacks ------------------------------------------------
    def on_failure(self, name, kind):
        self._rec("failure_detected", node=name, kind=kind)
        if kind == "crash":
            if name == self.leader:
                self._rec("leader_lost", node=name)
                self.elect()
            if self.auto_restart:
                dt = self.cluster.restart(name)
                self._rec("restarted", node=name, seconds=round(dt, 3))

    def on_recovery(self, name):
        self._rec("node_recovered", node=name)
        if self.leader and name != self.leader:
            try:
                h, p = self.cluster.addr(name)
                send_message(h, p, {"type": "coordinator", "leader": self.leader}, timeout=0.5)  # state resync
                self._rec("resynced", node=name, leader=self.leader)
            except Exception:
                pass
            # Bully rule: a recovered node that OUTRANKS the leader takes over.
            if self.cluster.configs[name].election_id > self.cluster.configs[self.leader].election_id:
                self.elect(initiator=name, among=self._alive(extra=[name]))


# =============================================================================
# Open-loop request stream
# =============================================================================

def run_open_loop(request_fn, rate_hz, duration_s, scheduled_events=(), max_workers=96):
    """
    Issues request_fn(i) at fixed times i/rate_hz, regardless of how long
    earlier requests take (open loop). `scheduled_events` is a list of
    (offset_seconds, callable) executed at the right moments on their own
    threads. Returns (t0, records) where each record has i, t_sent, t_done
    (seconds relative to t0) plus whatever request_fn returned.
    """
    records = []
    lock = threading.Lock()
    total = int(rate_hz * duration_s)
    events = sorted(scheduled_events, key=lambda e: e[0])
    ev_i = 0
    t0 = time.time()

    def one(i, t_sent):
        out = request_fn(i)
        out["i"] = i
        out["t_sent"] = round(t_sent, 4)
        out["t_done"] = round(time.time() - t0, 4)
        with lock:
            records.append(out)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for i in range(total):
            target = i / rate_hz
            while ev_i < len(events) and events[ev_i][0] <= target:
                threading.Thread(target=events[ev_i][1], daemon=True).start()
                ev_i += 1
            delay = target - (time.time() - t0)
            if delay > 0:
                time.sleep(delay)
            pool.submit(one, i, time.time() - t0)
        # fire any events scheduled after the last request
        while ev_i < len(events):
            threading.Thread(target=events[ev_i][1], daemon=True).start()
            ev_i += 1
    records.sort(key=lambda r: r["i"])
    return t0, records

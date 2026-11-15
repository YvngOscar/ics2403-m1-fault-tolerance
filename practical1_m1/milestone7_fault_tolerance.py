"""
================================================================================
PRACTICAL 1 | MILESTONE 7 - Fault Tolerance and Recovery (Week 7)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

WHAT THIS SCRIPT DEMONSTRATES (per the milestone brief):
  [M7-a] Fault-injection framework: five failure types injected on demand
         - CRASH         the node's OS process is killed
         - OMISSION      the node receives requests but never answers
         - COMMUNICATION the node's link becomes slow (400 ms delay)
         - SERVICE       the node is alive but its service returns errors
         - BYZANTINE     the node answers confidently but WRONGLY
  [M7-b] Failure DETECTION: heartbeat detector (100 ms interval, 3 misses)
         that also classifies WHAT kind of failure it saw.
  [M7-c] REPLICATION + FAILOVER: the session service runs on edge-1
         (primary), edge-2 and ran-1; clients fail over automatically.
         Byzantine faults are masked by QUORUM voting (majority wins).
  [M7-d] RECOVERY: a supervisor restarts a crashed node, re-syncs it, and
         re-elects the leader (Bully algorithm from Milestone 4) when the
         leader itself dies -- the "automatic trigger" Milestone 4 promised.
  [M7-e] Measurements: Availability = T_uptime / T_total, MTTR,
         detection time, and a failure-by-failure comparison of
         "no fault tolerance" vs "with fault tolerance".

Run:
    python -m practical1_m7_m8.milestone7_fault_tolerance
================================================================================
"""

import sys
import os
import json
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.cluster import Cluster
from common.communication import send_message
from common.fault_tolerance import (FaultInjector, FailureDetector, ReplicatedClient,
                                    Supervisor, run_open_loop)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# ---- experiment parameters (named constants so a second researcher can reproduce) ----
RATE_HZ = 40             # client requests per second (open loop)
STREAM_S = 5.0           # length of every failure experiment
INJECT_AT = 1.0          # seconds into the stream when the fault is injected
FAULT_S = 2.0            # how long a transient fault lasts / how long a human takes to repair a crash
TARGET = "edge-1"        # the node we break (it is the primary replica)
REPLICAS = ["edge-1", "edge-2", "ran-1"]
CLIENT_TIMEOUT_S = 0.25

FAULT_PARAMS = {
    "crash": {},
    "omission": {"probability": 1.0, "hold_s": 3.0},
    "delay": {"delay_ms": 400},
    "service_down": {},
    "byzantine": {},
}
RECOVERY_ACTION = {
    "crash": "supervisor restarts the node + failover to a replica",
    "omission": "failover to a replica; detector flags the node as unresponsive",
    "delay": "failover to a replica after the timeout; detector flags the slow node",
    "service_down": "service health probe flags it; failover to a replica",
    "byzantine": "quorum voting outvotes the lying node (no detection by heartbeat is possible)",
}


def log(tag, msg):
    print(f"[{tag}] {msg}")


def make_task(i):
    return {"name": f"session-{i}", "service_class": "URLLC",
            "cpu": 0.5, "memory": 16, "bandwidth": 1, "work_units": 5}


def ensure_healthy(cluster, injector):
    """Bring every node back to a known-good state between experiments."""
    for name in cluster.configs:
        if not cluster.is_process_alive(name):
            cluster.restart(name)
    for name in cluster.configs:
        try:
            injector.clear(name, "omission")   # sends fault_clear (works for any transient fault)
        except Exception:
            pass
    time.sleep(0.3)


def run_failure_case(cluster, kind, fault_tolerant):
    injector = FaultInjector(cluster)
    ensure_healthy(cluster, injector)

    detector = supervisor = None
    if fault_tolerant:
        supervisor = Supervisor(cluster, auto_restart=True)
        detector = FailureDetector(cluster, on_failure=supervisor.on_failure,
                                   on_recovery=supervisor.on_recovery)
        supervisor.detector = detector
        detector.start()
        time.sleep(0.4)  # let the detector establish a healthy baseline

    if not fault_tolerant:
        mode = "single"
    elif kind == "byzantine":
        mode = "quorum"
    else:
        mode = "failover"
    client = ReplicatedClient(cluster, REPLICAS, detector, mode=mode, timeout=CLIENT_TIMEOUT_S)

    marks = {}

    def do_inject():
        marks["inject"] = time.time()
        injector.inject(TARGET, kind, **FAULT_PARAMS[kind])

    def do_end_fault():
        marks["end"] = time.time()
        if kind == "crash":
            if not fault_tolerant:          # the HUMAN operator notices and restarts the node
                cluster.restart(TARGET)
                marks["manual_repair_done"] = time.time()
        else:
            injector.clear(TARGET, kind)

    t0, records = run_open_loop(
        lambda i: client.request(make_task(i)), RATE_HZ, STREAM_S,
        scheduled_events=[(INJECT_AT, do_inject), (INJECT_AT + FAULT_S, do_end_fault)])
    time.sleep(0.8)  # let recovery callbacks (restart/resync) finish and be recorded

    total = len(records)
    served = [r for r in records if r["ok"]]
    failed = total - len(served)
    incorrect = len([r for r in served if not r["correct"]])
    dt = 1.0 / RATE_HZ
    availability = len(served) / total
    correct_ratio = len([r for r in records if r["ok"] and r["correct"]]) / total
    failover_reqs = len([r for r in records if r.get("attempts", 1) > 1]) if mode == "failover" else 0
    msgs_per_req = round(sum(r.get("attempts", 1) for r in records) / total, 2)
    inject_abs = marks.get("inject", t0 + INJECT_AT)

    detection_s = None
    detected_as = None
    if fault_tolerant and kind != "byzantine":
        st = detector.state[TARGET]
        if st["suspected_at"]:
            detection_s = round(st["suspected_at"] - inject_abs, 3)
            detected_as = st["kind"]
    if kind == "byzantine" and fault_tolerant:
        dissent = [r for r in records if r.get("dissenters")]
        if dissent:
            detection_s = round(t0 + dissent[0]["t_done"] - inject_abs, 3)
            detected_as = "dissenting replica: " + ",".join(dissent[0]["dissenters"])

    node_mttr = None
    if kind == "crash":
        if fault_tolerant and detector.state[TARGET]["recovered_at"]:
            node_mttr = round(detector.state[TARGET]["recovered_at"] - inject_abs, 3)
        elif not fault_tolerant and "manual_repair_done" in marks:
            node_mttr = round(marks["manual_repair_done"] - inject_abs, 3)

    lat_ok = sorted(r["latency_ms"] for r in served)
    p95 = lat_ok[int(0.95 * (len(lat_ok) - 1))] if lat_ok else None

    if fault_tolerant:
        detector.stop()
    ensure_healthy(cluster, injector)

    return {
        "failure_type": kind,
        "fault_tolerance": "with FT" if fault_tolerant else "none",
        "client_mode": mode,
        "total_requests": total,
        "served": len(served),
        "failed_requests": failed,
        "incorrect_results_accepted": incorrect,
        "availability": round(availability, 4),
        "correct_service_ratio": round(correct_ratio, 4),
        "client_visible_downtime_s": round(failed * dt, 3),
        "detection_time_s": detection_s,
        "detected_as": detected_as,
        "node_mttr_s": node_mttr,
        "requests_needing_failover": failover_reqs,
        "messages_per_request": msgs_per_req,
        "p95_latency_ms": p95,
        "recovery_action": RECOVERY_ACTION[kind] if fault_tolerant else
                           ("human operator restarts the node" if kind == "crash" else "fault simply ends"),
        "supervisor_events": [{"t_rel": round(e["t"] - inject_abs, 3), **{k: v for k, v in e.items() if k != "t"}}
                              for e in (supervisor.events if supervisor else [])],
    }


def failure_comparison(cluster):
    log("M7", f"Failure comparison: 5 failure types x (no fault tolerance | with fault tolerance), "
               f"{int(STREAM_S)}s streams at {RATE_HZ} requests/s, fault injected at t={INJECT_AT}s for {FAULT_S}s")
    rows = []
    for kind in ("crash", "omission", "delay", "service_down", "byzantine"):
        for ft in (False, True):
            r = run_failure_case(cluster, kind, ft)
            rows.append(r)
            extra = f"  incorrect_accepted={r['incorrect_results_accepted']}" if kind == "byzantine" else ""
            log("M7", f"  {kind:12s} {r['fault_tolerance']:7s} availability={r['availability']*100:6.2f}%  "
                       f"failed={r['failed_requests']:3d}  detect={r['detection_time_s']}s{extra}")
    return rows


def leader_failure_experiment(cluster):
    log("M7", "Leader-failure experiment: kill the elected leader (cloud-1) and watch automatic re-election ...")
    injector = FaultInjector(cluster)
    ensure_healthy(cluster, injector)
    sup = Supervisor(cluster, auto_restart=True)
    det = FailureDetector(cluster, on_failure=sup.on_failure, on_recovery=sup.on_recovery)
    sup.detector = det
    # Initial election, started by the LOWEST-ranked node (worst case), as in Milestone 4.
    first = sup.elect(initiator="edge-1", among=list(cluster.configs))
    log("M7", f"  initial leader: {first['leader']} (rounds={first['rounds']}, messages={first['messages']}, "
               f"consistent={first['consistent']})")
    det.start()
    time.sleep(0.4)
    crash_at = time.time()
    injector.inject("cloud-1", "crash")
    log("M7", "  cloud-1 (the leader) crashed ...")

    deadline = time.time() + 12
    while time.time() < deadline:
        elected = [e for e in sup.events if e["event"] == "elected"]
        if len(elected) >= 3:      # initial election + re-election after crash + leadership reclaimed
            break
        time.sleep(0.1)
    time.sleep(0.5)
    det.stop()

    elected = [e for e in sup.events if e["event"] == "elected"]
    restarted = [e for e in sup.events if e["event"] == "restarted"]
    detected = [e for e in sup.events if e["event"] == "failure_detected"]
    result = {
        "initial_leader": first["leader"],
        "detection_time_s": round(detected[0]["t"] - crash_at, 3) if detected else None,
        "detected_as": detected[0]["kind"] if detected else None,
        "reelection_after_crash": None,
        "restart_seconds": restarted[0]["seconds"] if restarted else None,
        "leader_reclaimed_by_recovered_node": None,
        "final_views": {},
    }
    # elected[0] is the INITIAL election recorded by sup.elect() above
    if len(elected) >= 2:
        e = elected[1]
        result["reelection_after_crash"] = {
            "new_leader": e["leader"], "rounds": e["rounds"], "messages": e["messages"],
            "consistent_across_alive_nodes": e["consistent"],
            "seconds_after_crash": round(e["t"] - crash_at, 3)}
    if len(elected) >= 3:
        e = elected[2]
        result["leader_reclaimed_by_recovered_node"] = {
            "leader": e["leader"], "rounds": e["rounds"], "messages": e["messages"],
            "consistent": e["consistent"], "seconds_after_crash": round(e["t"] - crash_at, 3)}
    for n in cluster.configs:
        try:
            h, p = cluster.addr(n)
            result["final_views"][n] = send_message(h, p, {"type": "leader_query"}, timeout=1.0).get("current_leader")
        except Exception:
            result["final_views"][n] = None
    result["final_leader_consistent"] = len(set(result["final_views"].values())) == 1
    log("M7", f"  detected as '{result['detected_as']}' after {result['detection_time_s']}s")
    if result["reelection_after_crash"]:
        r = result["reelection_after_crash"]
        log("M7", f"  re-elected {r['new_leader']} {r['seconds_after_crash']}s after the crash "
                   f"(rounds={r['rounds']}, messages={r['messages']}, consistent={r['consistent_across_alive_nodes']})")
    if result["leader_reclaimed_by_recovered_node"]:
        r = result["leader_reclaimed_by_recovered_node"]
        log("M7", f"  cloud-1 restarted in {result['restart_seconds']}s and reclaimed leadership "
                   f"(Bully rule: highest rank wins) -> leader={r['leader']}, consistent={r['consistent']}")
    log("M7", f"  final views of the leader across all nodes: {result['final_views']}")
    return result


def availability_model():
    """
    Replication maths. If ONE replica is up a fraction `a` of the time, and the
    system works whenever AT LEAST ONE of r independent replicas is up:
        A_system = 1 - (1 - a)^r
    """
    rows = []
    for a in (0.95, 0.99, 0.999):
        for r in (1, 2, 3):
            A = 1 - (1 - a) ** r
            downtime_h_per_year = (1 - A) * 365 * 24
            rows.append({"single_replica_availability": a, "replicas": r,
                         "system_availability": round(A, 8),
                         "downtime_hours_per_year": round(downtime_h_per_year, 4)})
    return rows


def run(write_json=True):
    cluster = Cluster()
    cluster.start_all()
    try:
        comparison = failure_comparison(cluster)
        leader = leader_failure_experiment(cluster)
    finally:
        cluster.stop_all()
    model = availability_model()
    out = {
        "parameters": {"rate_hz": RATE_HZ, "stream_s": STREAM_S, "inject_at_s": INJECT_AT,
                       "fault_duration_s": FAULT_S, "target_node": TARGET, "replicas": REPLICAS,
                       "client_timeout_s": CLIENT_TIMEOUT_S, "heartbeat_interval_s": 0.1,
                       "misses_to_suspect": 3},
        "failure_comparison": comparison,
        "leader_failure": leader,
        "availability_model": model,
    }
    if write_json:
        path = os.path.join(RESULTS_DIR, "milestone7_fault_tolerance.json")
        with open(path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        log("M7", f"Results saved -> {path}")
    return out


def main():
    run()
    log("M7", "MILESTONE 7 COMPLETE.")


if __name__ == "__main__":
    main()

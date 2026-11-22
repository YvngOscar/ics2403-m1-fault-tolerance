"""
================================================================================
PRACTICAL 2 | MILESTONE 10 - Naming and Distributed Process Management (Week 10)
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)

PART 1 - NAMING:  Name -> Name Resolution -> Resource Location
  [M10-a] Five resolution architectures over the real platform (static map,
          broadcast, central name server, prefix table, DNS-style).
  [M10-b] Resolution latency and messages ("exchanges") per lookup.
  [M10-c] Scalability: lookups per second with 16 concurrent clients; effect of
          the number of names; message cost as the number of nodes grows.
  [M10-d] What happens when resources MOVE (staleness and update cost) and when
          a node DIES (the name server's node).

PART 2 - DISTRIBUTED PROCESS MANAGEMENT:  Create -> Locate -> Schedule -> Coordinate -> Terminate
  [M10-e] Per-stage latency of the full lifecycle (200 processes) and a leak check.
  [M10-f] Three scheduling policies compared.
  [M10-g] Creation throughput with 1, 4, 16 concurrent creators.
  [M10-h] COORDINATE: atomic group start (Two-Phase Commit), forced atomic abort,
          moving a running process (make-before-break vs break-before-make), and
          automatic respawn of the processes of a crashed node.

Run:
    python -m practical2_m9_m10.milestone10_naming_process
================================================================================
"""

import sys
import os
import json
import math
import time
import random
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.cluster import Cluster
from common.communication import send_message
from common.fault_tolerance import FailureDetector
from common.naming import (NameWorld, make_names, SCHEMES, RESOLVER_CLASSES, DnsStyleResolver,
                           PrefixTableResolver, workload_exchanges)
from common.process_mgmt import Scheduler, DistributedProcessManager, POLICIES
from common.system_models import SIM_RTT_MS, TIER_OF, _normalise
from common.workload_generator import generate_workload

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

N_NAMES = 1000
ZIPF_S = 1.1
ROUNDS = 3
LABELS = {"static_map": "Static map", "broadcast": "Broadcast", "name_server": "Central name server",
          "prefix_table": "Prefix table (zones)", "dns_style": "DNS-style (root + cache)"}
WEIGHTS = {"latency": 0.20, "exchanges": 0.10, "staleness": 0.20, "fault_tolerance": 0.20,
           "update_cost": 0.15, "scalability": 0.15}
EQUAL = {k: 1 / len(WEIGHTS) for k in WEIGHTS}
SCORE_DIRECTION = {"latency": False, "exchanges": False, "staleness": False, "fault_tolerance": True,
                   "update_cost": False, "scalability": False}


def log(tag, msg):
    print(f"[{tag}] {msg}")


def mean(v):
    return sum(v) / len(v) if v else None


def pct(v, q):
    s = sorted(v)
    return s[int(q * (len(s) - 1))] if s else None


def median(v):
    s = sorted(v)
    return s[len(s) // 2]


def zipf_sequence(names, n, seed):
    rng = random.Random(seed)
    weights = [1.0 / (i + 1) ** ZIPF_S for i in range(len(names))]
    return rng.choices(names, weights=weights, k=n)


# =============================================================================
# PART 1: NAMING
# =============================================================================

def timed_lookups(resolver, seq, truth):
    lat, ok = [], 0
    resolver.reset_counter()
    for n in seq:
        t = time.perf_counter()
        r = resolver.resolve(n)
        lat.append((time.perf_counter() - t) * 1e6)
        ok += int(r == truth[n])
    return lat, ok, resolver.exchanges


def exp_latency(world, truth):
    log("M10", f"N1. Resolution latency and messages: 2000 lookups, Zipf-popular names, {ROUNDS} interleaved rounds ...")
    names = list(truth)
    seq = zipf_sequence(names, 2000, seed=1)
    acc = {s: {"mean": [], "p95": [], "ex": 0, "ok": 0, "hit": 0.0} for s in SCHEMES}
    for _ in range(ROUNDS):
        for s in SCHEMES:
            r = RESOLVER_CLASSES[s](world) if s != "dns_style" else DnsStyleResolver(world, ttl_s=30.0)
            lat, ok, ex = timed_lookups(r, seq, truth)
            a = acc[s]
            a["mean"].append(mean(lat)); a["p95"].append(pct(lat, .95)); a["ex"] = ex / len(seq); a["ok"] = ok / len(seq)
            if s == "dns_style":
                a["hit"] = r.cache_hits / r.lookups
    rows = {}
    for s in SCHEMES:
        a = acc[s]
        rows[s] = {"scheme": LABELS[s], "mean_latency_us": round(median(a["mean"]), 1), "p95_latency_us": round(median(a["p95"]), 1),
                   "exchanges_per_lookup": round(a["ex"], 3), "correct_fraction": round(a["ok"], 4),
                   "cache_hit_rate": round(a["hit"], 3) if s == "dns_style" else None}
        r = rows[s]
        log("M10", f"  {LABELS[s]:26s} mean {r['mean_latency_us']:8.1f} us   p95 {r['p95_latency_us']:8.1f} us   "
                    f"exchanges/lookup {r['exchanges_per_lookup']:6.3f}   correct {r['correct_fraction']*100:5.1f}%"
                    + (f"   cache hit rate {r['cache_hit_rate']*100:.1f}%" if s == "dns_style" else ""))
    return rows


def exp_throughput(world, truth):
    log("M10", "N2. Scalability under load: 16 concurrent clients x 150 lookups ...")
    seq = zipf_sequence(list(truth), 2400, seed=2)
    rows = {}
    for s in SCHEMES:
        r = RESOLVER_CLASSES[s](world) if s != "dns_style" else DnsStyleResolver(world, ttl_s=30.0)
        ok = [0] * 16

        def worker(k):
            for n in seq[k * 150:(k + 1) * 150]:
                ok[k] += int(r.resolve(n) == truth[n])

        ts = [threading.Thread(target=worker, args=(k,)) for k in range(16)]
        t0 = time.perf_counter()
        [t.start() for t in ts]
        [t.join() for t in ts]
        dt = time.perf_counter() - t0
        rows[s] = {"scheme": LABELS[s], "lookups_per_second": round(2400 / dt), "correct_fraction": round(sum(ok) / 2400, 4)}
        log("M10", f"  {LABELS[s]:26s} {rows[s]['lookups_per_second']:9,d} lookups/s   correct {rows[s]['correct_fraction']*100:5.1f}%")
    return rows


def exp_names_scaling(cluster):
    log("M10", "N3. Effect of the NUMBER OF NAMES (100 / 1,000 / 10,000) ...")
    out = []
    for n in (100, 1000, 10000):
        truth = make_names(n, seed=3)
        world = NameWorld(cluster)
        t0 = time.perf_counter()
        world.build(truth)
        build_s = time.perf_counter() - t0
        seq = zipf_sequence(list(truth), 300, seed=4)
        row = {"names": n, "registration_seconds": round(build_s, 3)}
        for s in ("name_server", "prefix_table", "broadcast"):
            lat, ok, ex = timed_lookups(RESOLVER_CLASSES[s](world), seq, truth)
            row[f"{s}_mean_us"] = round(mean(lat), 1)
        out.append(row)
        log("M10", f"  {n:6d} names: registration {build_s:6.3f}s   lookup mean (us): server {row['name_server_mean_us']}, "
                    f"prefix {row['prefix_table_mean_us']}, broadcast {row['broadcast_mean_us']}")
    return out


def exp_staleness(world, truth):
    log("M10", "N4. When resources MOVE: staleness and update cost (100 resources move /edge -> cloud-1) ...")
    movers = [n for n in truth if n.startswith("/edge")][:100]
    warm = movers[:50]                                   # the DNS cache happens to hold half of them
    res = {s: (RESOLVER_CLASSES[s](world) if s != "dns_style" else DnsStyleResolver(world, ttl_s=1.0)) for s in SCHEMES}
    for n in warm:
        res["dns_style"].resolve(n)
    world.relocate(movers, "cloud-1")                    # the physical move; no name table knows yet
    stale = lambda s: sum(res[s].resolve(n) != world.truth[n] for n in movers) / len(movers)
    # "before update" is measured with a THROWAWAY cold-cache DNS resolver: measuring through the real one would
    # itself fill its cache with the old locations and distort the "after update" result.
    before = {s: (stale(s) if s != "dns_style" else
                  sum(DnsStyleResolver(world, ttl_s=1.0).resolve(n) != world.truth[n] for n in movers) / len(movers))
              for s in SCHEMES}
    upd = {s: sum(res[s].update(n, "cloud-1") for n in movers) / len(movers) for s in SCHEMES}
    after = {s: stale(s) for s in SCHEMES}
    time.sleep(1.2)                                      # longer than the 1-second TTL
    after_ttl = {s: stale(s) for s in SCHEMES}
    rows = {}
    for s in SCHEMES:
        rows[s] = {"scheme": LABELS[s], "stale_before_update": before[s], "stale_after_update": after[s],
                   "stale_after_ttl": after_ttl[s], "update_exchanges_per_move": upd[s]}
        r = rows[s]
        log("M10", f"  {LABELS[s]:26s} stale right after move {before[s]*100:5.1f}%   after name-server update {after[s]*100:5.1f}%   "
                    f"after TTL {after_ttl[s]*100:5.1f}%   update cost {upd[s]:4.1f} exchanges/move")
    return rows


def exp_fault(cluster, world, truth):
    log("M10", "N5. When a NODE DIES: kill core-1 (central name server, root, /core and /proc zones) ...")
    names = list(truth)
    warm, test = zipf_sequence(names, 500, seed=5), zipf_sequence(names, 500, seed=6)
    res = {s: (RESOLVER_CLASSES[s](world) if s != "dns_style" else DnsStyleResolver(world, ttl_s=30.0)) for s in SCHEMES}
    for n in warm:
        res["dns_style"].resolve(n)
    cluster.kill("core-1")
    share_core = sum(1 for n in test if truth[n] == "core-1") / len(test)
    rows = {}
    for s in SCHEMES:
        ok = sum(1 for n in test if res[s].resolve(n) == truth[n]) / len(test)
        rows[s] = {"scheme": LABELS[s], "resolution_success_with_core1_dead": round(ok, 4)}
        log("M10", f"  {LABELS[s]:26s} resolution success {ok*100:5.1f}%")
    log("M10", f"  (for reference, {share_core*100:.1f}% of the looked-up names are hosted on core-1 itself)")
    cluster.restart("core-1")
    return rows, share_core


def naming_scorecard(latency, staleness, fault, dns_hit):
    metrics = {k: {} for k in WEIGHTS}
    for s in SCHEMES:
        metrics["latency"][s] = latency[s]["mean_latency_us"]
        metrics["exchanges"][s] = latency[s]["exchanges_per_lookup"]
        metrics["staleness"][s] = staleness[s]["stale_after_update"]
        metrics["fault_tolerance"][s] = fault[s]["resolution_success_with_core1_dead"]
        metrics["update_cost"][s] = staleness[s]["update_exchanges_per_move"]
        metrics["scalability"][s] = workload_exchanges(s, nodes=500, clients=5000, lookups=1000, moves=10,
                                                       dns_hit_rate=dns_hit)
    LOG_SCALED = ("latency", "exchanges", "update_cost", "scalability")   # quantities spanning orders of magnitude

    def scores(weights, use_log):
        out = {}
        for k in WEIGHTS:
            vals = {s: (math.log10(1 + metrics[k][s]) if (use_log and k in LOG_SCALED) else metrics[k][s]) for s in SCHEMES}
            out[k] = _normalise(vals, SCORE_DIRECTION[k])
        return {s: round(sum(weights[k] * out[k][s] for k in WEIGHTS), 4) for s in SCHEMES}

    return metrics, {"weighted_linear": scores(WEIGHTS, False), "equal_linear": scores(EQUAL, False),
                     "weighted_log": scores(WEIGHTS, True), "equal_log": scores(EQUAL, True)}


def part1_naming(cluster):
    truth = make_names(N_NAMES, seed=10)
    world = NameWorld(cluster)
    world.build(truth)
    latency = exp_latency(world, truth)
    throughput = exp_throughput(world, truth)
    names_scaling = exp_names_scaling(cluster)
    world.build(truth)
    staleness = exp_staleness(world, truth)
    world.build(truth)
    fault, share_core = exp_fault(cluster, world, truth)
    metrics, sc = naming_scorecard(latency, staleness, fault, latency["dns_style"]["cache_hit_rate"])
    ranks = {k: sorted(v, key=lambda s: -v[s]) for k, v in sc.items()}
    log("M10", "N6. Naming scorecard (weights: latency .20, staleness .20, fault tolerance .20, update cost .15, "
                "scalability .15, messages .10). Ranking under four scoring variants:")
    for k in ("weighted_linear", "weighted_log", "equal_linear", "equal_log"):
        log("M10", f"  {k:16s} " + "  >  ".join(f"{LABELS[s]} ({sc[k][s]:.2f})" for s in ranks[k]))
    return {"latency": latency, "throughput": throughput, "names_scaling": names_scaling, "staleness": staleness,
            "fault_tolerance": fault, "share_of_lookups_hosted_on_failed_node": share_core,
            "scorecard_metrics": metrics, "scorecards": sc, "rankings": ranks}


# =============================================================================
# PART 2: PROCESS MANAGEMENT
# =============================================================================

def process_specs(n, seed):
    out = []
    for t in generate_workload(n, seed=seed):
        out.append({"service_class": t["service_class"], "cpu": t["cpu"], "memory": t["memory"],
                    "bandwidth": t["bandwidth"], "work_units": t["work_units"]})
    return out


def utilization_leak_check(cluster):
    cpu, running = {}, 0
    for n in cluster.configs:
        cpu[n] = send_message(*cluster.addr(n), {"type": "metrics_request"})["resource_utilization"]["cpu"]
        running += len(send_message(*cluster.addr(n), {"type": "proc_list"})["running"])
    return {"max_cpu_utilization_after": max(cpu.values()), "processes_still_running": running}


def exp_lifecycle(cluster, pm):
    log("M10", "P1. Lifecycle of 200 processes (8 waves of 25): create -> schedule -> start/register -> locate -> terminate ...")
    specs = process_specs(200, seed=1010)
    stage = {k: [] for k in ("create", "schedule", "start", "register", "locate", "terminate")}
    rejected = 0
    for w in range(8):
        names = []
        for i in range(25):
            name = f"/proc/life-{w}-{i}"
            r = pm.start(name, specs[w * 25 + i])
            if r["status"] != "started":
                rejected += 1
                continue
            names.append(name)
            for k, v in r["stages_ms"].items():
                stage[k].append(v)
        for name in names:
            node, ms = pm.locate(name)
            stage["locate"].append(ms)
        for name in names:
            r = pm.terminate(name)
            stage["terminate"].append(r["stages_ms"]["terminate"])
    rows = {k: {"mean_ms": round(mean(v), 3), "p95_ms": round(pct(v, .95), 3), "samples": len(v)} for k, v in stage.items()}
    leak = utilization_leak_check(cluster)
    for k, r in rows.items():
        log("M10", f"  {k:9s} mean {r['mean_ms']:7.3f} ms   p95 {r['p95_ms']:7.3f} ms   ({r['samples']} samples)")
    log("M10", f"  rejected (no capacity): {rejected}/200   leak check: max CPU utilization after = "
                f"{leak['max_cpu_utilization_after']}, processes still running = {leak['processes_still_running']}")
    return {"stages": rows, "rejected": rejected, "leak_check": leak}


def exp_policies(cluster, resolver):
    log("M10", "P2. Three scheduling policies: 25 mixed processes per wave, 3 waves ...")
    rows = {}
    for policy in POLICIES:
        sch = Scheduler(cluster, policy)
        pm = DistributedProcessManager(cluster, resolver, sch)
        jain, dist, urllc_dist, maxu, rej = [], [], [], [], 0
        for w in range(3):
            specs = process_specs(25, seed=2000 + w)
            names = []
            for i, sp in enumerate(specs):
                r = pm.start(f"/proc/pol-{policy}-{w}-{i}", sp)
                if r["status"] == "started":
                    names.append(f"/proc/pol-{policy}-{w}-{i}")
                    dist.append(SIM_RTT_MS[TIER_OF[r["node"]]])
                    if sp["service_class"] == "URLLC":
                        urllc_dist.append(SIM_RTT_MS[TIER_OF[r["node"]]])
                else:
                    rej += 1
            u = list(sch.utilization().values())
            jain.append(sum(u) ** 2 / (len(u) * sum(x * x for x in u)) if sum(x * x for x in u) else 1.0)
            maxu.append(max(u))
            for n in names:
                pm.terminate(n)
        rows[policy] = {"load_balance_jain_index": round(mean(jain), 3), "mean_distance_rtt_ms": round(mean(dist), 2),
                        "urllc_mean_distance_rtt_ms": round(mean(urllc_dist), 2),
                        "max_node_utilization": round(mean(maxu), 3), "rejected_processes": rej}
        r = rows[policy]
        log("M10", f"  {policy:15s} balance (Jain) {r['load_balance_jain_index']:.3f}   mean distance {r['mean_distance_rtt_ms']:5.2f} ms   "
                    f"URLLC distance {r['urllc_mean_distance_rtt_ms']:5.2f} ms   busiest node {r['max_node_utilization']*100:5.1f}%   rejected {rej}")
    return rows


def exp_creation_throughput(cluster, resolver):
    log("M10", "P3. Process creation throughput (start + terminate) with 1, 4, 16 concurrent creators ...")
    rows = []
    spec = {"service_class": "mMTC", "cpu": 0.5, "memory": 16, "bandwidth": 1, "work_units": 5}
    for creators in (1, 4, 16):
        pm = DistributedProcessManager(cluster, resolver, Scheduler(cluster, "location_aware"))
        done, fail = [0] * creators, [0] * creators

        def worker(k):
            for i in range(25):
                nm = f"/proc/thr-{creators}-{k}-{i}"
                if pm.start(nm, spec)["status"] == "started":
                    pm.terminate(nm)
                    done[k] += 1
                else:
                    fail[k] += 1

        ts = [threading.Thread(target=worker, args=(k,)) for k in range(creators)]
        t0 = time.perf_counter()
        [t.start() for t in ts]
        [t.join() for t in ts]
        dt = time.perf_counter() - t0
        rows.append({"concurrent_creators": creators, "lifecycles_per_second": round(sum(done) / dt, 1),
                     "completed": sum(done), "failed": sum(fail)})
        log("M10", f"  {creators:2d} creators: {rows[-1]['lifecycles_per_second']:7.1f} lifecycles/s   completed {sum(done)}  failed {sum(fail)}")
    return rows


def exp_group(cluster, resolver):
    log("M10", "P4. COORDINATE: atomic group start with Two-Phase Commit ...")
    pm = DistributedProcessManager(cluster, resolver, Scheduler(cluster, "location_aware"))
    spec = {"service_class": "eMBB", "cpu": 2.0, "memory": 64, "bandwidth": 5, "work_units": 5}
    members = [("/proc/pipe-auth", spec), ("/proc/pipe-qos", spec), ("/proc/pipe-billing", spec)]
    ok = pm.start_group("pipeline-ok", members)
    located = {n: pm.locate(n)[0] for n, _ in members}
    pm.terminate_group("pipeline-ok")
    leak_ok = utilization_leak_check(cluster)

    # Force a VOTE-ABORT: untracked ("hidden") load fills the nodes the scheduler is about to choose.
    hidden = {"edge-1": 7.0, "edge-2": 7.0, "ran-1": 11.0}
    for n, cpu in hidden.items():
        send_message(*cluster.addr(n), {"type": "proc_start", "name": f"hidden-{n}",
                                        "spec": {"cpu": cpu, "memory": 64, "bandwidth": 1}})
    ab = pm.start_group("pipeline-abort", members)
    started_any = sum(len([x for x in send_message(*cluster.addr(n), {"type": "proc_list"})["running"] if x.startswith("/proc/pipe")])
                      for n in cluster.configs)
    cpu_after = {n: send_message(*cluster.addr(n), {"type": "metrics_request"})["resource_utilization"]["cpu"] for n in hidden}
    expected = {n: round(hidden[n] / cluster.configs[n].cpu_capacity, 4) for n in hidden}
    for n in hidden:
        send_message(*cluster.addr(n), {"type": "proc_terminate", "name": f"hidden-{n}"})
    out = {"normal_group": {"status": ok["status"], "twopc_ms": round(ok["twopc_ms"], 3), "placements": ok["placements"],
                            "all_members_locatable_by_name": all(v is not None for v in located.values()),
                            "leak_check_after_terminate": leak_ok},
           "forced_abort": {"status": ab["status"], "reason": ab.get("reason"), "votes": ab.get("votes"),
                            "group_processes_running_anywhere": started_any,
                            "cpu_utilization_after_abort": cpu_after, "hidden_load_only_expected": expected,
                            "no_resources_leaked": cpu_after == expected}}
    log("M10", f"  normal group: {ok['status']} on {ok['placements']} (2PC {ok['twopc_ms']:.2f} ms); leak after stop: {leak_ok}")
    log("M10", f"  forced abort: {ab['status']} ({ab.get('reason')}), votes {ab.get('votes')}; processes running anywhere: {started_any}; "
                f"resources leaked: {not out['forced_abort']['no_resources_leaked']}")
    return out


def poll_locates(pm, name, stop, counters):
    while not stop.is_set():
        node, _ = pm.locate(name)
        up = False
        if node is not None:
            try:
                up = send_message(*pm.cluster.addr(node), {"type": "proc_status", "name": name}, timeout=0.5)["status"] == "running"
            except Exception:
                up = False
        counters["polls"] += 1
        counters["failed"] += 0 if up else 1
        time.sleep(0.002)


def exp_migration(cluster, resolver):
    log("M10", "P5. COORDINATE: move a running process (make-before-break vs break-before-make) ...")
    rows = {}
    spec = {"service_class": "URLLC", "cpu": 1.0, "memory": 32, "bandwidth": 2, "work_units": 5}
    for mbb in (True, False):
        pm = DistributedProcessManager(cluster, resolver, Scheduler(cluster, "location_aware"), state_transfer_s=0.05)
        name = "/proc/mig-" + ("mbb" if mbb else "bbm")
        pm.start(name, spec)
        src = pm.procs[name]["node"]
        dst = "edge-2" if src != "edge-2" else "edge-1"
        stop, counters = threading.Event(), {"polls": 0, "failed": 0}
        th = threading.Thread(target=poll_locates, args=(pm, name, stop, counters))
        th.start()
        time.sleep(0.2)
        r = pm.migrate(name, dst, make_before_break=mbb)
        time.sleep(0.2)
        stop.set()
        th.join()
        final = pm.locate(name)[0]
        pm.terminate(name)
        key = "make_before_break" if mbb else "break_before_make"
        rows[key] = {"moved_from": src, "moved_to": dst, "migration_seconds": round(r["seconds"], 3),
                     "polls": counters["polls"], "failed_locates": counters["failed"],
                     "estimated_downtime_ms": counters["failed"] * 2 + (0 if mbb else 0),
                     "name_resolves_to_new_node_after": final == dst}
        log("M10", f"  {key:18s} took {r['seconds']:.3f}s   {counters['failed']} of {counters['polls']} locate-and-check polls found the service DOWN")
    return rows


def exp_respawn(cluster, resolver):
    log("M10", "P6. COORDINATE: crash a node hosting processes; detector triggers automatic respawn elsewhere ...")
    pm = DistributedProcessManager(cluster, resolver, Scheduler(cluster, "round_robin"))
    specs = process_specs(12, seed=3030)
    for i, sp in enumerate(specs):
        sp["cpu"] = min(sp["cpu"], 1.5)
        pm.start(f"/proc/svc-{i}", sp)
    per_node = {}
    for n, r in pm.procs.items():
        per_node[r["node"]] = per_node.get(r["node"], 0) + 1
    victim = max((n for n in per_node if n != "core-1"), key=lambda n: per_node[n])
    marks = {}

    def on_failure(node, kind):
        if kind == "crash" and node == victim:
            marks["detected"] = time.time()
            marks["result"] = pm.respawn_lost(node)
            marks["restored"] = time.time()

    det = FailureDetector(cluster, on_failure=on_failure)
    det.start()
    time.sleep(0.4)
    marks["crash"] = time.time()
    cluster.kill(victim)
    deadline = time.time() + 10
    while "restored" not in marks and time.time() < deadline:
        time.sleep(0.05)
    det.stop()
    consistent = 0
    for name, rec in pm.procs.items():
        node, _ = pm.locate(name)
        try:
            alive = node is not None and node != victim and \
                send_message(*cluster.addr(node), {"type": "proc_status", "name": name})["status"] == "running"
        except Exception:
            alive = False
        consistent += int(alive)
    out = {"processes_before": len(pm.procs), "processes_on_victim": per_node[victim], "victim_node": victim,
           "detection_seconds": round(marks["detected"] - marks["crash"], 3) if "detected" in marks else None,
           "all_restored_after_seconds": round(marks["restored"] - marks["crash"], 3) if "restored" in marks else None,
           "restored": marks.get("result", {}).get("restored"), "failed_to_restore": marks.get("result", {}).get("failed"),
           "names_resolving_to_a_live_running_process": consistent, "names_total": len(pm.procs)}
    log("M10", f"  killed {victim} (hosted {out['processes_on_victim']} of {out['processes_before']} processes): detected after "
                f"{out['detection_seconds']}s, all {out['restored']} restored after {out['all_restored_after_seconds']}s; "
                f"{consistent}/{len(pm.procs)} names resolve to a live running process")
    cluster.restart(victim)
    pm.scheduler.revive(victim)
    for name in list(pm.procs):
        try:
            pm.terminate(name)
        except Exception:
            pass
    return out


def part2_process(cluster):
    world = NameWorld(cluster)
    world.build({})                                    # empty name space; referrals for the /proc zone installed
    resolver = PrefixTableResolver(world)              # the zone architecture handles /proc names
    pm = DistributedProcessManager(cluster, resolver, Scheduler(cluster, "location_aware"))
    return {"naming_scheme_used": resolver.scheme,
            "lifecycle": exp_lifecycle(cluster, pm), "policies": exp_policies(cluster, resolver),
            "creation_throughput": exp_creation_throughput(cluster, resolver),
            "group": exp_group(cluster, resolver), "migration": exp_migration(cluster, resolver),
            "respawn": exp_respawn(cluster, resolver)}


def run(write_json=True):
    cluster = Cluster()
    cluster.start_all()
    try:
        naming = part1_naming(cluster)
        process = part2_process(cluster)
    finally:
        cluster.stop_all()
    out = {"parameters": {"names": N_NAMES, "zipf_s": ZIPF_S, "rounds": ROUNDS, "zones": {"/edge": "edge-1", "/ran": "ran-1",
                          "/core": "core-1", "/cloud": "cloud-1", "/proc": "core-1"}, "weights": WEIGHTS},
           "naming": naming, "process_management": process}
    if write_json:
        path = os.path.join(RESULTS_DIR, "milestone10_naming_process.json")
        with open(path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        log("M10", f"Results saved -> {path}")
    return out


def main():
    run()
    log("M10", "MILESTONE 10 COMPLETE.")


if __name__ == "__main__":
    main()

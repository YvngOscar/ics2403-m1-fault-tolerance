"""
integrity.py
-------------
[MILESTONE 7 - Fault Tolerance and Recovery]

A tiny, deterministic "result" function for tasks. Every node that
executes a task returns task_checksum(task) as its answer. A healthy
node always returns the right value; a node suffering a BYZANTINE
fault returns a plausible-looking but WRONG value. Because the correct
answer can be recomputed by anyone, the platform (and the experiments)
can tell when a node has quietly lied -- which a simple "is the node
alive?" check can never reveal.
"""

import zlib


def task_checksum(task: dict) -> int:
    key = f"{task['name']}:{task['work_units']}".encode("utf-8")
    return zlib.crc32(key)

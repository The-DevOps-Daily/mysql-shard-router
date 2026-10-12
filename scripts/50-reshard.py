"""Split shard c0- in two while writes keep coming, Vitess Reshard style.

  c0-  (port 3314)  ->  c0-e0 (stays on 3314) + e0- (new, port 3315)

1. A writer thread inserts messages the whole time and records every error.
2. Start mirroring writes for the moving range e0- to the new shard
   (the double-write that stands in for VReplication's binlog stream).
3. Copy the existing e0- rows in chunks of 2,000.
4. Verify: row count and checksum of every e0- row up to a watermark id must
   match on both sides (what VDiff does).
5. Switch: route e0- to the new shard in one atomic map change.
6. Clean up the moved rows on the old shard, stop the writer and check that
   every row exists exactly once, on the right shard.
Writes results/reshard.json and the new shardmap.json.
"""
import json, pathlib, random, threading, time

from router import Router
from shardmap import keyspace_id, in_range

r = Router()
SRC, DST, MOVING = 3314, 3315, "e0-"
LO = bytes.fromhex("e000000000000000")
weights = [1 / (i + 1) for i in range(2000)]
log = {"writes": 0, "errors": 0, "lat_ms": []}
phase = ["before"]
stop = threading.Event()

def writer():
    g = random.Random(5)
    mid = 1_000_000
    while not stop.is_set():
        mid += 1
        ch = g.choices(range(1, 2001), weights=weights, k=1)[0]
        t = time.perf_counter()
        try:
            r.insert_message(mid, ch, g.randint(1, 5000), f"live message {mid}", "2026-10-12 10:00:00")
            log["writes"] += 1
        except Exception as e:                       # record, keep going
            log["errors"] += 1
        log["lat_ms"].append((time.time(), (time.perf_counter() - t) * 1000, phase[0]))
        time.sleep(0.005)

for t in ("messages", "message_lookup", "accounts"):    # the new shard starts empty, same schema
    ddl = r.run(SRC, f"SHOW CREATE TABLE {t}")[0][1]
    r.run(DST, f"DROP TABLE IF EXISTS {t}")
    r.run(DST, ddl)
w = threading.Thread(target=writer); w.start()
time.sleep(3)
phases = {}

# 2. mirror new writes for the moving range
with r.lock:
    r.mirror = (MOVING, DST)
phase[0] = "copy"
time.sleep(0.5)                                       # let an in-flight write land

# 3. copy existing rows
t0 = time.time(); copied, last = 0, 0
while True:
    rows = r.run(SRC, "SELECT id, ksid, channel_id, user_id, body, created_at FROM messages "
                      "WHERE ksid >= %s AND id > %s ORDER BY id LIMIT 2000", (LO, last))
    if not rows:
        break
    with r.conn(DST).cursor() as cur:
        cur.executemany("INSERT IGNORE INTO messages (id, ksid, channel_id, user_id, body, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s)", rows)
    copied += len(rows); last = rows[-1][0]
phases["copy_seconds"] = round(time.time() - t0, 2)
phases["rows_copied"] = copied

# 4. verify up to a watermark
phase[0] = "verify"
wm = r.run(SRC, "SELECT MAX(id) FROM messages")[0][0]
time.sleep(0.5)
q = ("SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', id, channel_id, user_id, body))), 0) "
     "FROM messages WHERE ksid >= %s AND id <= %s")
src_sum, dst_sum = r.run(SRC, q, (LO, wm))[0], r.run(DST, q, (LO, wm))[0]
phases["verify"] = {"watermark_id": wm, "source": [int(x) for x in src_sum], "target": [int(x) for x in dst_sum],
                    "match": tuple(src_sum) == tuple(dst_sum)}
assert phases["verify"]["match"], phases["verify"]

# 5. switch routing in one step
phase[0] = "after switch"
t_switch = time.time()
with r.lock:
    shards = dict(r.map.shards)
    del shards["c0-"]
    shards["c0-e0"], shards["e0-"] = SRC, DST
    r.map.shards = shards
    r.mirror = None
phases["switch_ms"] = round((time.time() - t_switch) * 1000, 3)
time.sleep(3)

# 6. clean up and check
phase[0] = "cleanup"
t0 = time.time(); deleted = 0
while True:
    with r.conn(SRC).cursor() as cur:
        n = cur.execute("DELETE FROM messages WHERE ksid >= %s LIMIT 2000", (LO,))
    deleted += n
    if n == 0:
        break
phases["cleanup_seconds"] = round(time.time() - t0, 2); phases["rows_deleted_from_source"] = deleted
stop.set(); w.join()

total = sum(r.run(p, "SELECT COUNT(*) FROM messages")[0][0] for p in r.map.shards.values())
misplaced = 0
for name, port in r.map.shards.items():
    for (ksid,) in r.run(port, "SELECT ksid FROM messages"):
        if not in_range(bytes(ksid), name):
            misplaced += 1
lat = sorted(l for _, l, _ in log["lat_ms"])
around = [l for ts, l, _ in log["lat_ms"] if abs(ts - t_switch) < 1]
by_phase = {}
for name in ("before", "copy", "verify", "after switch", "cleanup"):
    xs = sorted(l for _, l, p in log["lat_ms"] if p == name)
    if xs:
        by_phase[name] = {"writes": len(xs), "p50": round(xs[len(xs) // 2], 2), "p99": round(xs[int(len(xs) * .99)], 2), "max": round(xs[-1], 2)}
out = {
    "phases": phases,
    "writes_during_run": log["writes"], "write_errors": log["errors"],
    "write_latency_ms": {"p50": round(lat[len(lat) // 2], 2), "p99": round(lat[int(len(lat) * .99)], 2), "max": round(lat[-1], 2),
                         "max_within_1s_of_switch": round(max(around), 2) if around else None, "by_phase": by_phase},
    "rows_total_after": int(total), "rows_expected": 200_000 + log["writes"], "rows_on_wrong_shard": misplaced,
    "naive_cutover_write_downtime_seconds": phases["copy_seconds"],
    "shards_after": r.map.shards,
}
r.map.save()
print(json.dumps(out, indent=2))
pathlib.Path("/work/results/reshard.json").write_text(json.dumps(out, indent=2) + "\n")

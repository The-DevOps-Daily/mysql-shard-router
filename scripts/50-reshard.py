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
All three tables move: messages (keyed by channel), message_lookup (keyed by
message id) and accounts (keyed by account id). A reader thread runs scatter
and lookup-vindex reads the whole time and counts missing or duplicate rows.
This is an insert-only demonstration: the copy is not a consistent snapshot and
the mirror has no update/delete ordering or crash replay, which VReplication has.
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
reads = {"reads": 0, "missing": 0, "duplicates": 0}

def reader():
    g = random.Random(9)
    while not stop.is_set():
        mid = g.randint(1, 200_000)
        if len(r.by_id_lookup(mid)) != 1:
            reads["missing"] += 1
        rows = r.by_user(g.randint(1, 5000))
        if len({row[0] for row in rows}) != len(rows):
            reads["duplicates"] += 1
        reads["reads"] += 2
        time.sleep(0.01)

def moving_rows(table):
    # message_lookup and accounts do not store the keyspace ID, so compute it.
    return [row for row in r.run(SRC, f"SELECT * FROM {table}") if in_range(keyspace_id(row[0]), MOVING)]

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
rd = threading.Thread(target=reader); rd.start()
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
moved_small = {}
for table, ncols in (("message_lookup", 2), ("accounts", 2)):
    rows = moving_rows(table)
    with r.conn(DST).cursor() as cur:
        cur.executemany(f"INSERT IGNORE INTO {table} VALUES ({','.join(['%s'] * ncols)})", rows)
    moved_small[table] = len(rows)
phases["copy_seconds"] = round(time.time() - t0, 2)
phases["rows_copied"] = {"messages": copied, **moved_small}

# 4. verify up to a watermark
phase[0] = "verify"
wm = r.run(SRC, "SELECT MAX(id) FROM messages")[0][0]
time.sleep(0.5)
q = ("SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', id, channel_id, user_id, body))), 0) "
     "FROM messages WHERE ksid >= %s AND id <= %s")
src_sum, dst_sum = r.run(SRC, q, (LO, wm))[0], r.run(DST, q, (LO, wm))[0]
def small_sum(port, table):
    rows = [row for row in r.run(port, f"SELECT * FROM {table}") if in_range(keyspace_id(row[0]), MOVING) and (table != "message_lookup" or row[0] <= wm)]
    return [len(rows), hash(tuple(sorted(rows)))]
phases["verify"] = {"watermark_id": wm,
                    "messages_count_and_crc32_xor": {"source": [int(x) for x in src_sum], "target": [int(x) for x in dst_sum]},
                    "lookup_and_accounts_full_row_compare": {t: small_sum(SRC, t) == small_sum(DST, t) for t in ("message_lookup", "accounts")}}
phases["verify"]["match"] = tuple(src_sum) == tuple(dst_sum) and all(phases["verify"]["lookup_and_accounts_full_row_compare"].values())
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
phases["local_map_update_ms"] = round((time.time() - t_switch) * 1000, 3)
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
for table, col in (("message_lookup", "id"), ("accounts", "account_id")):
    ids = [row[0] for row in r.run(SRC, f"SELECT {col} FROM {table}") if in_range(keyspace_id(row[0]), MOVING)]
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        r.run(SRC, f"DELETE FROM {table} WHERE {col} IN ({','.join(['%s'] * len(chunk))})", chunk)
phases["cleanup_seconds"] = round(time.time() - t0, 2); phases["rows_deleted_from_source"] = deleted
stop.set(); w.join(); rd.join()

total = sum(r.run(p, "SELECT COUNT(*) FROM messages")[0][0] for p in r.map.shards.values())
misplaced = 0
ids_seen, dup_ids = set(), 0
for name, port in r.map.shards.items():
    for mid, ksid, ch in r.run(port, "SELECT id, ksid, channel_id FROM messages"):
        if not in_range(bytes(ksid), name) or bytes(ksid) != keyspace_id(ch):
            misplaced += 1
        dup_ids += mid in ids_seen; ids_seen.add(mid)
    for (k,) in r.run(port, "SELECT id FROM message_lookup") + r.run(port, "SELECT account_id FROM accounts"):
        if not in_range(keyspace_id(k), name):
            misplaced += 1
expected_ids = set(range(1, 200_001)) | set(range(1_000_001, 1_000_001 + log["writes"]))
lookups = sum(r.run(p, "SELECT COUNT(*) FROM message_lookup")[0][0] for p in r.map.shards.values())
accounts = sum(r.run(p, "SELECT COUNT(*) FROM accounts")[0][0] for p in r.map.shards.values())
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
    "messages_after": int(total), "messages_expected": 200_000 + log["writes"],
    "message_ids_match_expected_set": ids_seen == expected_ids, "duplicate_message_ids": int(dup_ids),
    "lookup_rows_after": int(lookups), "accounts_after": int(accounts),
    "rows_on_wrong_shard_or_bad_ksid": misplaced,
    "reads_during_run": reads,
    "shards_after": r.map.shards,
}
r.map.save()
print(json.dumps(out, indent=2))
pathlib.Path("/work/results/reshard.json").write_text(json.dumps(out, indent=2) + "\n")

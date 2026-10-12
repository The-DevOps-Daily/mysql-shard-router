"""Targeted vs lookup-vindex vs scatter reads.

Two pairs that return the same rows:
  A. one message by id: the app knows the channel (1 shard query), a lookup
     vindex finds it (2), or the router scatters to every shard (4).
  B. the latest 50 messages: by channel, the sharding key (1 shard), or by
     user, which is not the sharding key (every shard, then merge).
Part 1, latency: 1,000 of each query type, one at a time.
Part 2, capacity: 8 client threads for 15 seconds per query type, counting
logical queries and the SQL statements that actually hit shards.
Writes results/reads.json.
"""
import json, pathlib, random, statistics, threading, time

from router import Router

r = Router()
rng = random.Random(7)
N_CH, N_USERS, N_MSG = 2000, 5000, 200_000
weights = [1 / (i + 1) for i in range(N_CH)]

def pick_channel(g): return g.choices(range(1, N_CH + 1), weights=weights, k=1)[0]

# The same 2,000 random message ids for all three "A" queries, with their
# channel fetched up front (not timed) for the targeted case.
known = []
for mid in random.Random(3).sample(range(1, N_MSG + 1), 2000):
    known.append((mid, r.run(r.port_for(mid), "SELECT channel_id FROM message_lookup WHERE id=%s", (mid,))[0][0]))

QUERIES = {
    "A. message by id, channel known (targeted)": (lambda g: r.by_id_targeted(*g.choice(known))),
    "A. message by id, lookup vindex":            (lambda g: r.by_id_lookup(g.choice(known)[0])),
    "A. message by id, scatter":                  (lambda g: r.by_id_scatter(g.choice(known)[0])),
    "B. latest 50 by channel (targeted)":         (lambda g: r.by_channel(pick_channel(g))),
    "B. latest 50 by user (scatter + merge)":     (lambda g: r.by_user(g.randint(1, N_USERS))),
}

def pct(xs, p):
    xs = sorted(xs); return xs[min(len(xs) - 1, int(len(xs) * p / 100))]

out = {"latency_ms": {}, "capacity_15s_8_threads": {}}
# Warm every query type over the whole id set first, then interleave the types
# in random order so no type gets a colder or warmer cache than another.
for mid, ch in known:
    r.by_id_targeted(mid, ch); r.by_id_lookup(mid); r.by_id_scatter(mid)
for _ in range(200):
    for q in QUERIES.values():
        q(rng)
samples = {name: [] for name in QUERIES}
counts = {name: 0 for name in QUERIES}
for _ in range(1000):
    for name in rng.sample(list(QUERIES), len(QUERIES)):
        before = r.shard_queries
        t = time.perf_counter(); QUERIES[name](rng); samples[name].append((time.perf_counter() - t) * 1000)
        counts[name] += r.shard_queries - before
for name, xs in samples.items():
    out["latency_ms"][name] = {
        "p50": round(statistics.median(xs), 2), "p95": round(pct(xs, 95), 2),
        "p99": round(pct(xs, 99), 2), "shard_queries_per_query": round(counts[name] / 1000, 2),
    }
    print(name, out["latency_ms"][name], flush=True)

for name, q in QUERIES.items():
    stop = time.time() + 15
    done = [0] * 8
    before = r.shard_queries
    def worker(i):
        g = random.Random(100 + i)
        while time.time() < stop:
            q(g); done[i] += 1
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    total = sum(done)
    out["capacity_15s_8_threads"][name] = {
        "logical_qps": round(total / 15, 1), "shard_qps": round((r.shard_queries - before) / 15, 1),
        "router_cpu_note": "router, client threads and all MySQL servers share one 4-core Pi",
    }
    print(name, out["capacity_15s_8_threads"][name], flush=True)

pathlib.Path("/work/results/reads.json").write_text(json.dumps(out, indent=2) + "\n")

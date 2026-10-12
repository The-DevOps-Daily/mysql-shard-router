"""What a cross-shard transaction does when something fails halfway.

Each transfer moves 10 credits between two accounts. When both accounts are on
the same shard it is one MySQL transaction. When they are on different shards
it is two transactions committed one after the other, which is what Vitess's
default transaction mode ("multi", no atomicity) does. We inject a failure in
2% of transfers: the process dies after the first commit and before the second.
Writes results/cross-shard.json.
"""
import json, pathlib, random, time

from router import Router

r = Router()
rng = random.Random(11)
FAIL_RATE, N = 0.02, 2000
accounts = list(range(1, 1001))

def total():
    return int(sum(r.run(p, "SELECT COALESCE(SUM(credits),0) FROM accounts")[0][0] for p in r.snapshot()[0].values()))

def tx(port):
    c = r.conn(port); c.begin(); return c

def run(mode):
    before, failures, times = total(), 0, []
    for _ in range(N):
        while True:
            a, b = rng.sample(accounts, 2)
            same = r.port_for(a) == r.port_for(b)
            if same == (mode == "same-shard"):
                break
        fail = rng.random() < FAIL_RATE
        t = time.perf_counter()
        if mode == "same-shard":
            c = tx(r.port_for(a))
            with c.cursor() as cur:
                cur.execute("UPDATE accounts SET credits = credits - 10 WHERE account_id=%s", (a,))
                cur.execute("UPDATE accounts SET credits = credits + 10 WHERE account_id=%s", (b,))
            if fail:
                c.rollback(); failures += 1      # crash before commit: nothing applied
            else:
                c.commit()
        else:
            ca, cb = tx(r.port_for(a)), tx(r.port_for(b))
            with ca.cursor() as cur:
                cur.execute("UPDATE accounts SET credits = credits - 10 WHERE account_id=%s", (a,))
            with cb.cursor() as cur:
                cur.execute("UPDATE accounts SET credits = credits + 10 WHERE account_id=%s", (b,))
            ca.commit()                            # first shard committed
            if fail:
                cb.rollback(); failures += 1      # crash: second shard never commits
            else:
                cb.commit()
        times.append((time.perf_counter() - t) * 1000)
    after = total()
    times.sort()
    return {"transfers": N, "injected_failures": failures, "credits_before": before, "credits_after": after,
            "credits_lost": before - after, "p50_ms": round(times[N // 2], 2), "p99_ms": round(times[int(N * 0.99)], 2)}

for p in r.snapshot()[0].values():
    r.run(p, "UPDATE accounts SET credits = 1000")   # start every run from the same state

out = {"same-shard": run("same-shard"), "cross-shard": run("cross-shard")}
print(json.dumps(out, indent=2))
pathlib.Path("/work/results/cross-shard.json").write_text(json.dumps(out, indent=2) + "\n")

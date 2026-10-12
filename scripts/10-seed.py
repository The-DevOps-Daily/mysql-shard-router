"""Create the schema on every shard and load the dataset.

200,000 messages in 2,000 channels. Channel activity follows a Zipf curve (a
few channels are very busy, most are quiet), which is what real chat, ticket
and order data looks like. Messages are sharded by channel_id, like Slack's
messages keyspace. Writes results/seed.json.
"""
import collections, datetime, json, pathlib, random, time

from router import Router
from shardmap import keyspace_id, in_range

N_MESSAGES, N_CHANNELS, N_USERS, N_ACCOUNTS = 200_000, 2_000, 5_000, 1_000
SCHEMA = [
    "DROP TABLE IF EXISTS messages, message_lookup, accounts",
    """CREATE TABLE messages (
         id BIGINT PRIMARY KEY, ksid BINARY(8) NOT NULL, channel_id BIGINT NOT NULL,
         user_id BIGINT NOT NULL, body VARCHAR(200) NOT NULL, created_at DATETIME NOT NULL,
         KEY by_channel (channel_id, id), KEY by_user (user_id, id), KEY by_ksid (ksid))""",
    # Lookup vindex table: sharded by message id, maps it to the channel.
    "CREATE TABLE message_lookup (id BIGINT PRIMARY KEY, channel_id BIGINT NOT NULL)",
    "CREATE TABLE accounts (account_id BIGINT PRIMARY KEY, credits BIGINT NOT NULL)",
]

# Always start from four shards; 50-reshard.py leaves five behind.
pathlib.Path("/work/shardmap.json").write_text(json.dumps({"shards": {"-40": 3311, "40-80": 3312, "80-c0": 3313, "c0-": 3314}}, indent=2) + "\n")
rng = random.Random(42)
weights = [1 / (rank + 1) for rank in range(N_CHANNELS)]          # Zipf, s = 1
channels = list(range(1, N_CHANNELS + 1))
r = Router()
shards, _ = r.snapshot()
for port in shards.values():
    for stmt in SCHEMA:
        r.run(port, stmt)

t0 = time.time()
per_shard = collections.defaultdict(list)
lookups = collections.defaultdict(list)
start = datetime.datetime(2026, 9, 1)
picked = rng.choices(channels, weights=weights, k=N_MESSAGES)
for mid, ch in enumerate(picked, start=1):
    user = rng.randint(1, N_USERS)
    ksid = keyspace_id(ch)
    per_shard[r.port_for(ch)].append((mid, ksid, ch, user, f"message {mid} in channel {ch}", start + datetime.timedelta(seconds=mid * 13)))
    lookups[r.port_for(mid)].append((mid, ch))

def bulk(port, sql, rows):
    for i in range(0, len(rows), 2000):
        with r.conn(port).cursor() as cur:
            cur.executemany(sql, rows[i:i + 2000])

for port, rows in per_shard.items():
    bulk(port, "INSERT INTO messages (id, ksid, channel_id, user_id, body, created_at) VALUES (%s,%s,%s,%s,%s,%s)", rows)
for port, rows in lookups.items():
    bulk(port, "INSERT INTO message_lookup (id, channel_id) VALUES (%s,%s)", rows)
acc = collections.defaultdict(list)
for a in range(1, N_ACCOUNTS + 1):
    acc[r.port_for(a)].append((a, 1000))
for port, rows in acc.items():
    bulk(port, "INSERT INTO accounts (account_id, credits) VALUES (%s,%s)", rows)

counts = collections.Counter(picked)
top_channel, top_rows = counts.most_common(1)[0]
result = {
    "messages": N_MESSAGES, "channels": N_CHANNELS, "users": N_USERS, "load_seconds": round(time.time() - t0, 1),
    "rows_per_shard": {name: len(per_shard[port]) for name, port in shards.items()},
    "share_per_shard_pct": {name: round(100 * len(per_shard[port]) / N_MESSAGES, 1) for name, port in shards.items()},
    "top_channel": top_channel, "top_channel_rows": top_rows,
    "top_channel_share_pct": round(100 * top_rows / N_MESSAGES, 1),
    "top_channel_shard": r.map.shard_for(top_channel),
    "top10_channels_share_pct": round(100 * sum(c for _, c in counts.most_common(10)) / N_MESSAGES, 1),
}
pathlib.Path("/work/results/seed.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))

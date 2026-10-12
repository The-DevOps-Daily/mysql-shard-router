"""Does adding shards fix a hot shard? Count rows per channel from the live
shards, then work out the busiest shard's share for 2 to 128 equal keyspace
ranges, with and without splitting the busiest channel into 16 buckets.
Writes results/hot-shard.json.
"""
import collections, json, pathlib

from router import Router
from shardmap import keyspace_id

r = Router()
per_channel = collections.Counter()
for port in r.snapshot()[0].values():
    for ch, n in r.run(port, "SELECT channel_id, COUNT(*) FROM messages WHERE id <= 200000 GROUP BY channel_id"):
        per_channel[ch] += n
total = sum(per_channel.values())
hot, hot_rows = per_channel.most_common(1)[0]

def max_share(keys_rows, n):
    load = [0] * n
    for key, rows in keys_rows:
        idx = int.from_bytes(keyspace_id(key), "big") * n >> 64   # equal ranges of the 64-bit keyspace
        load[idx] += rows
    return round(100 * max(load) / total, 2)

plain = list(per_channel.items())
# Sub-shard the hot channel: its messages get 16 sharding keys (channel, bucket),
# e.g. bucket = message_id % 16, encoded here as distinct synthetic keys.
bucketed = [(k, v) for k, v in plain if k != hot] + [((hot << 8) + b + 10_000_000, hot_rows / 16) for b in range(16)]
out = {"total_rows": total, "hot_channel": hot, "hot_channel_share_pct": round(100 * hot_rows / total, 2), "by_shard_count": []}
for n in (2, 4, 8, 16, 32, 64, 128):
    out["by_shard_count"].append({"shards": n, "ideal_pct": round(100 / n, 2),
                                  "busiest_shard_pct": max_share(plain, n),
                                  "busiest_shard_pct_hot_channel_in_16_buckets": max_share(bucketed, n)})
print(json.dumps(out, indent=2))
pathlib.Path("/work/results/hot-shard.json").write_text(json.dumps(out, indent=2) + "\n")

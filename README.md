# mysql-shard-router

A small Vitess-style shard router over five real MySQL 8.4 servers, built to measure the primitives every large MySQL shop ends up with: a hash vindex, targeted vs scatter queries, a lookup vindex, cross-shard transactions, an online reshard, and hot shards.

It is not Vitess. The router is about 170 lines of Python, so you can read every decision. The numbers come from running it.

```bash
scripts/00-setup.sh                      # Ubuntu 26.04 chroot with mysql-server + python3-pymysql, 5 mysqld on 127.0.0.1:3311-3315
scripts/run.sh 10-seed.py                # schema + 200,000 messages in 2,000 channels (Zipf), sharded by channel_id
scripts/run.sh 20-reads.py               # targeted vs lookup vindex vs scatter: latency and capacity
scripts/run.sh 40-cross-shard-txn.py     # 2,000 transfers, 2% injected failures, same-shard vs cross-shard
scripts/run.sh 50-reshard.py             # split c0- into c0-e0 + e0- while a writer keeps inserting
scripts/run.sh 60-hot-shard.py           # busiest shard share for 2 to 128 shards
scripts/99-stop.sh                       # stop MySQL, unmount the chroot
```

Run `10-seed.py` again before re-running the other scripts: the reshard moves data and leaves five shards in `shardmap.json`.

Needs Linux with `sudo`, `curl` and about 3 GB of disk. The setup downloads the `arm64` Ubuntu base image (it was run on a Raspberry Pi 4); change `IMG` in `scripts/00-setup.sh` for `amd64`. Nothing is installed on the host.

## How it maps to Vitess

| Here | Vitess |
|---|---|
| `shardmap.json`, ranges like `-40`, `c0-` | keyspace shards with keyrange names |
| `keyspace_id()` (blake2b, 8 bytes) | the primary vindex (`hash`, `xxhash`) |
| `Router` | VTGate |
| `message_lookup` table | a lookup vindex |
| `Router.mirror` + chunked copy + checksum | VReplication copy and catch-up, then VDiff |
| one atomic shard map swap | `SwitchTraffic` |
| two commits in a row | the default `multi` transaction mode |

## Limits

All five MySQL servers share one Raspberry Pi (4 cores, SD card), so absolute latencies are higher and noisier than on real hosts, and every shard competes for the same CPU. The ratios between query types are the useful part. Commits wait for the SD card, which is why write p99 sits in the hundreds of milliseconds.

## Results

`results/*.json`. The write-up is on DevOps Daily (linked from the repo description).

License: MIT

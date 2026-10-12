"""Query routing on top of ShardMap: targeted, lookup-vindex and scatter reads,
plus writes that can be mirrored to a second shard while a range is moving."""
import threading
from concurrent.futures import ThreadPoolExecutor

import pymysql

from shardmap import ShardMap, keyspace_id, in_range

COLS = "id, ksid, channel_id, user_id, body, created_at"


class Router:
    def __init__(self):
        self.map = ShardMap()
        self.lock = threading.Lock()
        self.mirror = None            # (range_name, target_port) while a range is being copied
        self.local = threading.local()
        self.pool = ThreadPoolExecutor(max_workers=8)
        self.shard_queries = 0        # every SQL statement sent to a shard

    # one connection per (thread, port): PyMySQL connections are not thread-safe
    def conn(self, port):
        conns = getattr(self.local, "conns", None)
        if conns is None:
            conns = self.local.conns = {}
        c = conns.get(port)
        if c is None:
            c = pymysql.connect(host="127.0.0.1", port=port, user="root", database="app", autocommit=True)
            conns[port] = c
        return c

    def run(self, port, sql, args=None):
        with self.lock:
            self.shard_queries += 1
        with self.conn(port).cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()

    def snapshot(self):
        with self.lock:
            return dict(self.map.shards), self.mirror

    def port_for(self, key):
        shards, _ = self.snapshot()
        ksid = keyspace_id(key)
        for name, port in shards.items():
            if in_range(ksid, name):
                return port
        raise KeyError(key)

    # --- writes -----------------------------------------------------------
    def insert_message(self, mid, channel_id, user_id, body, created_at):
        ksid = keyspace_id(channel_id)
        shards, mirror = self.snapshot()
        port = next(p for n, p in shards.items() if in_range(ksid, n))
        sql = f"INSERT INTO messages ({COLS}) VALUES (%s,%s,%s,%s,%s,%s)"
        args = (mid, ksid, channel_id, user_id, body, created_at)
        self.run(port, sql, args)
        if mirror and in_range(ksid, mirror[0]):
            # Double-write into the shard the range is moving to, like Slack's
            # migration and like VReplication's catch-up stream.
            self.run(mirror[1], sql.replace("INSERT", "INSERT IGNORE", 1), args)

    # --- reads ------------------------------------------------------------
    def by_channel(self, channel_id, limit=50):
        return self.run(self.port_for(channel_id),
                        f"SELECT {COLS} FROM messages WHERE channel_id=%s ORDER BY id DESC LIMIT %s",
                        (channel_id, limit))

    def scatter(self, sql, args, merge_key=None, limit=None):
        shards, _ = self.snapshot()
        futures = [self.pool.submit(self.run, port, sql, args) for port in shards.values()]
        rows = [r for f in futures for r in f.result()]
        if merge_key:
            rows.sort(key=merge_key, reverse=True)
        return rows[:limit] if limit else rows

    def by_user(self, user_id, limit=50):
        return self.scatter(f"SELECT {COLS} FROM messages WHERE user_id=%s ORDER BY id DESC LIMIT %s",
                            (user_id, limit), merge_key=lambda r: r[0], limit=limit)

    def by_id_targeted(self, mid, channel_id):
        # The application already knows the sharding key (the channel).
        return self.run(self.port_for(channel_id), f"SELECT {COLS} FROM messages WHERE id=%s", (mid,))

    def by_id_scatter(self, mid):
        return self.scatter(f"SELECT {COLS} FROM messages WHERE id=%s", (mid,))

    def by_id_lookup(self, mid):
        # Lookup vindex: a table, itself sharded by message id, that maps the id
        # to its channel. One extra read, then a targeted query.
        row = self.run(self.port_for(mid), "SELECT channel_id FROM message_lookup WHERE id=%s", (mid,))
        if not row:
            return []
        return self.run(self.port_for(row[0][0]), f"SELECT {COLS} FROM messages WHERE id=%s", (mid,))

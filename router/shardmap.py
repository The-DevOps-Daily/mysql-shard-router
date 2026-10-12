"""A minimal Vitess-style shard map.

Each row's sharding key (here: channel_id) goes through a hash "vindex" to an
8-byte keyspace ID. Shards own half-open ranges of that ID space, written the
way Vitess writes them: "-40" is [0x00, 0x40), "c0-" is [0xc0, end).
"""
import hashlib
import json
import pathlib

import pymysql

ROOT = pathlib.Path(__file__).resolve().parent.parent
MAP_FILE = ROOT / "shardmap.json"


def keyspace_id(key: int) -> bytes:
    # Stand-in for Vitess's hash/xxhash vindex: any stable, well-mixed 64-bit hash works.
    return hashlib.blake2b(str(key).encode(), digest_size=8).digest()


def parse_range(name: str):
    lo, hi = name.split("-")
    lo_b = bytes.fromhex(lo.ljust(16, "0")) if lo else b"\x00" * 8
    hi_b = bytes.fromhex(hi.ljust(16, "0")) if hi else None
    return lo_b, hi_b


def in_range(ksid: bytes, name: str) -> bool:
    lo, hi = parse_range(name)
    return ksid >= lo and (hi is None or ksid < hi)


class ShardMap:
    def __init__(self, path=MAP_FILE):
        self.path = path
        self.load()

    def load(self):
        data = json.loads(self.path.read_text())
        self.shards = data["shards"]          # {"-40": 3311, ...}
        self._conns = {}

    def save(self):
        self.path.write_text(json.dumps({"shards": self.shards}, indent=2) + "\n")

    def conn(self, port: int):
        c = self._conns.get(port)
        if c is None:
            c = pymysql.connect(host="127.0.0.1", port=port, user="root", database="app", autocommit=True)
            self._conns[port] = c
        return c

    def shard_for(self, key: int) -> str:
        ksid = keyspace_id(key)
        for name in self.shards:
            if in_range(ksid, name):
                return name
        raise KeyError(key)

    def port_for(self, key: int) -> int:
        return self.shards[self.shard_for(key)]

    def all_ports(self):
        return list(self.shards.values())

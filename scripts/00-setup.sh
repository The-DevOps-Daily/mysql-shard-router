#!/usr/bin/env bash
# Build an Ubuntu 26.04 rootfs with MySQL and PyMySQL, then start one mysqld per
# shard on 127.0.0.1 (ports 3311-3314, plus 3315 for the reshard target).
# Everything runs inside the chroot, so nothing is installed on the host.
set -euo pipefail
cd "$(dirname "$0")/.."
R=$PWD/rootfs
IMG=ubuntu-base-26.04.1-base-arm64.tar.gz   # use the amd64 image on x86
BASE=https://cdimage.ubuntu.com/ubuntu-base/releases/26.04/release
if [ ! -x "$R/usr/bin/apt-get" ]; then
  mkdir -p downloads
  [ -f "downloads/$IMG" ] || curl -fsS -o "downloads/$IMG" "$BASE/$IMG"
  curl -fsS "$BASE/SHA256SUMS" | grep " \*\?$IMG\$" | sed 's/ \*/  /' | (cd downloads && sha256sum -c -)
  sudo mkdir -p "$R" && sudo tar -xzf "downloads/$IMG" -C "$R"
fi
sudo cp /etc/resolv.conf "$R/etc/resolv.conf"
mountpoint -q "$R/proc" || sudo mount -t proc proc "$R/proc"
mountpoint -q "$R/dev" || { sudo mount --bind /dev "$R/dev"; sudo mount -o remount,bind,ro "$R/dev"; }
sudo mkdir -p "$R/work" && sudo mount --bind "$PWD" "$R/work" 2>/dev/null || true

sudo chroot "$R" /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin DEBIAN_FRONTEND=noninteractive /bin/bash -c '
  set -e
  command -v mysqld >/dev/null || { apt-get update -qq; apt-get install -y -qq --no-install-recommends mysql-server python3 python3-pymysql >/dev/null; }
  mysqld --version
  for port in 3311 3312 3313 3314 3315; do
    d=/var/lib/shards/$port
    if [ ! -d $d/mysql ]; then
      mkdir -p $d && chown mysql:mysql $d
      mysqld --no-defaults --user=mysql --initialize-insecure --datadir=$d --innodb-redo-log-capacity=8388608 >/dev/null 2>&1
    fi
    if ! mysqladmin --protocol=tcp -h127.0.0.1 -P$port -uroot ping >/dev/null 2>&1; then
      setsid nohup mysqld --no-defaults --user=mysql --datadir=$d --port=$port --bind-address=127.0.0.1 \
        --socket=/tmp/mysql-$port.sock --mysqlx=OFF --pid-file=/tmp/mysql-$port.pid \
        --innodb-buffer-pool-size=32M --innodb-redo-log-capacity=8388608 --innodb-log-buffer-size=1M \
        --key-buffer-size=1M --table-open-cache=64 --table-definition-cache=400 --temptable-max-ram=8M \
        --innodb-adaptive-hash-index=OFF --performance-schema=OFF --skip-log-bin \
        --max-connections=30 --log-error=$d/error.log </dev/null >/dev/null 2>&1 &
    fi
  done
  for port in 3311 3312 3313 3314 3315; do
    for i in $(seq 1 60); do mysqladmin --protocol=tcp -h127.0.0.1 -P$port -uroot ping >/dev/null 2>&1 && break; sleep 1; done
    mysql --protocol=tcp -h127.0.0.1 -P$port -uroot -e "CREATE DATABASE IF NOT EXISTS app"
    echo "shard on $port is up"
  done
'

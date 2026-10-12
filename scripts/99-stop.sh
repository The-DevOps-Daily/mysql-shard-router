#!/usr/bin/env bash
# Stop every mysqld and unmount the chroot. Always run this before deleting
# rootfs/: rootfs/work is the repo itself, bind-mounted.
cd "$(dirname "$0")/.."
R=$PWD/rootfs
for port in 3311 3312 3313 3314 3315; do
  sudo chroot "$R" mysqladmin --protocol=tcp -h127.0.0.1 -P$port -uroot shutdown 2>/dev/null
done
sleep 2
for m in work dev proc; do while mountpoint -q "$R/$m"; do sudo umount -l "$R/$m"; done; done
echo stopped

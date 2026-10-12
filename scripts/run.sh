#!/usr/bin/env bash
# Run a demo script inside the chroot, where MySQL and PyMySQL live.
cd "$(dirname "$0")/.."
exec sudo chroot rootfs /usr/bin/env -i PATH=/usr/bin:/bin PYTHONPATH=/work/router PYTHONUNBUFFERED=1 python3 "/work/scripts/$1" "${@:2}"

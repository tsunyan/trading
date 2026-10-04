"""Read-only free-space check for stores required by a live dispatch."""

import shutil

MIN_FREE_BYTES = 2**30


class StorageCapacityError(ValueError):
    """Fixed local reasons without paths or operating-system error details."""


def require_capacity(directories):
    for directory in dict.fromkeys(directories):
        try:
            free = shutil.disk_usage(directory).free
        except OSError:
            raise StorageCapacityError("disk_space_unavailable") from None
        if free < MIN_FREE_BYTES:
            raise StorageCapacityError(f"disk_space_low:{free // 2**20}MiB")

"""Cleanup of files in a directory created exclusively by an initializer."""

from contextlib import contextmanager


@contextmanager
def new_storage_directory(directory, filenames):
    directory.mkdir(parents=True, exist_ok=False)
    identity = directory.stat()
    try:
        yield
    except BaseException:
        # Never recursively delete or touch a pre-existing/replaced directory.
        try:
            current = directory.stat()
            if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                for name in filenames:
                    try:
                        (directory / name).unlink(missing_ok=True)
                    except OSError:
                        pass
                directory.rmdir()  # Unknown files prevent removal.
        except OSError:
            pass
        raise

import sqlite3

import pytest

from trading.event_journal import EventJournal, JournalError
from trading.private_read import PrivateReadError
from trading.read_control import PersistentReadLimiter


@pytest.mark.parametrize(
    "factory,error", [(EventJournal, JournalError), (PersistentReadLimiter, PrivateReadError)]
)
@pytest.mark.parametrize("failure", ["connect", "schema"])
def test_failed_initialization_cleans_only_new_directory_and_can_retry(
    tmp_path, monkeypatch, factory, error, failure
):
    directory = tmp_path / "new-storage"
    real_connect = sqlite3.connect

    def broken(*args, **kwargs):
        if failure == "connect":
            raise sqlite3.OperationalError("fixture failure")
        conn = real_connect(*args, **kwargs)
        conn.execute("CREATE TABLE control (id INTEGER)")
        conn.execute("CREATE TABLE journal (id INTEGER)")
        conn.commit()
        return conn

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", broken)
        with pytest.raises(error, match="initialization_failed"):
            factory.create(directory, "synthetic")
    assert not directory.exists()
    assert factory.create(directory, "synthetic").path.is_file()


@pytest.mark.parametrize("factory", [EventJournal, PersistentReadLimiter])
def test_initialization_never_cleans_preexisting_directory(tmp_path, factory):
    marker = tmp_path / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    with pytest.raises(FileExistsError):
        factory.create(tmp_path, "synthetic")
    assert marker.read_text(encoding="utf-8") == "keep"

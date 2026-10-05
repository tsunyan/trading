"""Operator commands ride out the watchdog's brief owner probe; a live owner still refuses."""

import threading
import time

import pytest
from test_stream_control import begin, recovery
from test_stream_control import setup as control_setup

from trading.stream_control import OPERATOR_OWNER_WAIT_SECONDS, StreamControl, StreamControlError


@pytest.fixture
def setup(tmp_path):
    return control_setup.__wrapped__(tmp_path)


def hold(control, seconds, entered):
    with control.ownership():
        entered.set()
        time.sleep(seconds)


def test_recovery_waits_out_a_brief_probe_by_another_handle(setup):
    _, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
    probe = StreamControl(control.path.parent)
    entered = threading.Event()
    worker = threading.Thread(target=hold, args=(probe, 0.4, entered))
    worker.start()
    try:
        assert entered.wait(5)
        recovery(control, journal, book, acknowledge_token_uncertainty=True)
    finally:
        worker.join()
    assert control.snapshot()["phase"] == "READY"


def test_a_probe_that_never_releases_still_refuses_after_the_wait(setup):
    _, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
    probe = StreamControl(control.path.parent)
    entered = threading.Event()
    worker = threading.Thread(target=hold, args=(probe, OPERATOR_OWNER_WAIT_SECONDS + 1.0, entered))
    worker.start()
    try:
        assert entered.wait(5)
        started = time.monotonic()
        with pytest.raises(StreamControlError, match="owner_busy"):
            recovery(control, journal, book, acknowledge_token_uncertainty=True)
        assert time.monotonic() - started >= OPERATOR_OWNER_WAIT_SECONDS - 0.2
    finally:
        worker.join()


def test_the_default_acquisition_never_waits(setup):
    _, _, _, control = setup
    probe = StreamControl(control.path.parent)
    entered = threading.Event()
    worker = threading.Thread(target=hold, args=(probe, 0.5, entered))
    worker.start()
    try:
        assert entered.wait(5)
        started = time.monotonic()
        with pytest.raises(StreamControlError, match="owner_busy"):
            with control.ownership():
                pass
        assert time.monotonic() - started < 0.3
    finally:
        worker.join()

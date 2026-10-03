"""Offline private receiver demonstration. No credentials, sockets, or real waits."""

import argparse
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from pydantic import SecretStr

from trading.account_read_lab import demo_transcript, replay
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import PrivateStreamLimiter, PrivateTokenClient, StreamError


def demo(directory: Path):
    clock = [datetime(2026, 10, 2, tzinfo=UTC), 0.0]
    requests, subscriptions = [], []

    def advance(seconds):
        clock[0] += timedelta(seconds=seconds)
        clock[1] += seconds

    def token_response(request):
        requests.append({"method": request.method, "at": clock[0].isoformat()})
        return httpx.Response(
            200,
            json={
                "status": 0,
                "responsetime": clock[0].isoformat(),
                **({"data": "synthetic-not-a-real-token"} if request.method == "POST" else {}),
            },
        )

    class SyntheticSocket:
        pending = None

        def send(self, payload):
            subscriptions.append({"at": clock[0].isoformat(), **json.loads(payload)})

        def recv(self, *, timeout, decode):
            if self.pending is not None:
                payload, self.pending = self.pending, None
                return payload
            advance(timeout)
            raise TimeoutError

        def ping(self, *, ack_on_close):
            pong = threading.Event()
            pong.set()  # Synthetic receipt; production waits for the actual pong.
            return pong

        def close(self):
            pass

    journal = EventJournal.create(directory, "synthetic-stream")
    capture = JournaledEventCapture(
        journal, clock=lambda: clock[0], monotonic_ns=lambda: int(clock[1] * 1e9)
    )
    tokens = PrivateTokenClient(
        SecretStr("synthetic-key"),
        SecretStr("synthetic-secret"),
        limiter=PrivateStreamLimiter(monotonic=lambda: clock[1], sleep=advance),
        transport=httpx.MockTransport(token_response),
        clock=lambda: clock[0],
        monotonic=lambda: clock[1],
    )
    sock = SyntheticSocket()
    receiver = PrivateStreamReceiver(
        tokens, capture, connector=lambda _: sock, monotonic=lambda: clock[1]
    )
    steps = []

    def snapshot(label):
        steps.append({"step": label, **receiver.status()})

    try:
        receiver.start(expected_head=journal.inspect()["head"])
        snapshot("subscriptions_sent")
        receiver.resync(lambda: replay(demo_transcript(clock[0])))
        snapshot("rest_observed_unverified")
        sock.pending = json.dumps(
            {
                "channel": "positionEvents",
                "positionId": 401,
                "symbol": "USD_JPY",
                "side": "BUY",
                "size": "400",
                "orderdSize": "0",
                "price": "150",
                "lossGain": "-40",
                "totalSwap": "0",
                "timestamp": clock[0].isoformat(),
                "msgType": "UPR",
            }
        ).encode()
        receiver.step()
        snapshot("event_committed")
        for _ in range(95):
            advance(31)
            receiver.step()
            receiver.step()
        snapshot("quiet_connection_token_extended")
        advance(3570)  # No receive-loop service: neither token nor liveness revives.
        try:
            receiver.step()
        except StreamError:
            pass
        snapshot("expired_and_closed")
    finally:
        receiver.close()
    result = {
        "offline_only": True,
        "complete": False,
        "live_enabled": False,
        "token_requests": requests,
        "subscriptions": subscriptions,
        "steps": steps,
        "journal": journal.replay(),
    }
    with (directory / "report.json").open("x", encoding="utf-8") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("demo",))
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args(argv)
    result = demo(args.directory)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

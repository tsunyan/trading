"""Offline event/REST scenario replay. No sockets, keys, or changes to operational DBs."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field

from trading.account_read_lab import Transcript, demo_transcript
from trading.account_read_lab import replay as replay_account
from trading.account_sync import AccountSyncMonitor, SyncError
from trading.broker_contracts import Contract


class ControlStep(Contract):
    kind: Literal["connect", "disconnect", "heartbeat", "status"]
    at: AwareDatetime


class EventStep(Contract):
    kind: Literal["event"]
    at: AwareDatetime
    sequence: int = Field(strict=True, gt=0)
    payload: str = Field(max_length=16_384)


class ReadStep(Contract):
    kind: Literal["resync"]
    at: AwareDatetime
    transcript: Transcript


class SyncTranscript(Contract):
    version: Literal[1] = 1
    steps: tuple[
        Annotated[ControlStep | EventStep | ReadStep, Field(discriminator="kind")], ...
    ] = Field(min_length=1, max_length=2000)


def _unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_transcript_field")
        result[key] = value
    return result


def _nonfinite(value):
    raise ValueError("nonfinite_transcript_value")


def replay(transcript: SyncTranscript):
    transcript = SyncTranscript.model_validate(transcript.model_dump())
    origin = transcript.steps[0].at
    now = origin
    monitor = AccountSyncMonitor(
        clock=lambda: now, monotonic=lambda: (now - origin).total_seconds()
    )
    session = None
    results = []
    for index, step in enumerate(transcript.steps):
        now = step.at
        error = assessment = None
        try:
            if step.kind == "connect":
                session = monitor.start_session()
            elif step.kind == "disconnect":
                monitor.disconnect(session)
            elif step.kind == "heartbeat":
                monitor.heartbeat(session)
            elif step.kind == "event":
                monitor.ingest(session, step.sequence, step.payload.encode("utf-8"))
            elif step.kind == "resync":

                def collect(read_transcript=step.transcript):
                    nonlocal now
                    report = replay_account(read_transcript)
                    now = report.observations[-1].received_at
                    return report

                assessment = monitor.resync(session, collect).model_dump(mode="json")
        except SyncError as exc:
            error = str(exc)
        results.append(
            {
                "step": index,
                "operation": step.kind,
                "error": error,
                "status": monitor.status(),
                "assessment": assessment,
            }
        )
    return {"offline_only": True, "live_enabled": False, "steps": results}


def demo_transcript_events(now: datetime) -> SyncTranscript:
    def stamp(seconds):
        return now + timedelta(seconds=seconds)

    def reading(seconds, units):
        data = demo_transcript(stamp(seconds)).model_dump(mode="json")
        for exchange in data["exchanges"]:
            if exchange["path"] == "/v1/openPositions":
                for position in exchange["response"]["data"]["list"]:
                    position["size"] = str(units)
        return ReadStep(
            kind="resync", at=stamp(seconds), transcript=Transcript.model_validate(data)
        )

    def event(seconds, sequence, units):
        return EventStep(
            kind="event",
            at=stamp(seconds),
            sequence=sequence,
            payload=json.dumps(
                {
                    "channel": "positionEvents",
                    "positionId": 401,
                    "symbol": "USD_JPY",
                    "side": "BUY",
                    "size": str(units),
                    "orderdSize": "0",
                    "price": "150",
                    "lossGain": "-40",
                    "totalSwap": "0",
                    "timestamp": now.isoformat(),
                    "msgType": "UPR",
                }
            ),
        )

    return SyncTranscript(
        steps=(
            ControlStep(kind="connect", at=now),
            reading(1, 400),
            event(2, 1, 600),
            reading(3, 400),
            reading(4, 600),
            event(5, 3, 600),
            ControlStep(kind="connect", at=stamp(6)),
            reading(7, 600),
            ControlStep(kind="status", at=stamp(83)),
        )
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo")
    demo.add_argument("--directory", type=Path, required=True)
    replay_command = commands.add_parser("replay")
    replay_command.add_argument("--input", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            transcript = demo_transcript_events(datetime.now(UTC))
            result = replay(transcript)
            args.directory.mkdir(parents=True, exist_ok=False)
            (args.directory / "transcript.json").write_text(
                transcript.model_dump_json(indent=2), encoding="utf-8"
            )
            (args.directory / "report.json").write_text(
                json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            print(
                json.dumps(
                    {
                        "synthetic_only": True,
                        "live_enabled": False,
                        "steps": len(result["steps"]),
                        "phases": [r["status"]["phase"] for r in result["steps"]],
                    }
                )
            )
        else:
            with args.input.open("rb") as source:
                raw = source.read(2_000_001)
            if len(raw) > 2_000_000:
                raise ValueError("input_too_large")
            data = json.loads(raw, object_pairs_hook=_unique_fields, parse_constant=_nonfinite)
            print(json.dumps(replay(SyncTranscript.model_validate(data)), ensure_ascii=False))
    except (ValueError, OSError, RecursionError):
        parser.exit(2, "Account sync replay failed; check input and unused output directory.\n")


if __name__ == "__main__":
    main()

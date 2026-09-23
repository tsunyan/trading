import hashlib
import json
import subprocess
from pathlib import Path

from trading.config import Settings


def _git_output(root: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def source_sha256() -> str:
    package = Path(__file__).parent
    digest = hashlib.sha256()
    for path in sorted(package.rglob("*.py")):
        digest.update(path.relative_to(package).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def reproducibility_fields(
    cfg: Settings,
    data_sha256: str,
    swap_sha256: str | None,
    run_parameters: dict,
) -> dict:
    """Identity covers inputs, code and how the run was made (mode, folds, stress, warm-up)."""
    root = Path(__file__).resolve().parents[2]
    code_sha256 = source_sha256()
    git_commit = _git_output(root, "rev-parse", "HEAD")
    git_status = _git_output(root, "status", "--porcelain", "--untracked-files=all")
    identity = ":".join(
        [
            data_sha256,
            cfg.fingerprint,
            swap_sha256 or "no-swap",
            code_sha256,
            json.dumps(run_parameters, sort_keys=True),
        ]
    )
    return {
        "experiment_id": hashlib.sha256(identity.encode()).hexdigest(),
        "run_parameters": run_parameters,
        "code_sha256": code_sha256,
        "git_commit": git_commit,
        "git_dirty": bool(git_status) if git_status is not None else None,
    }

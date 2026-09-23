import hashlib
import importlib.metadata
import json
import platform
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


RUNTIME_PACKAGES = ("backtrader", "numpy", "pandas", "pyarrow", "pydantic")


def runtime_versions() -> dict:
    """Python and the installed versions of the packages that shape fills and numbers."""
    versions = {"python": platform.python_version()}
    for name in RUNTIME_PACKAGES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def git_state() -> dict:
    """Commit and dirty flag of the checkout. Take it before writing run artifacts, or the
    run's own output files would mark the worktree dirty."""
    root = Path(__file__).resolve().parents[2]
    git_status = _git_output(root, "status", "--porcelain", "--untracked-files=all")
    return {
        "git_commit": _git_output(root, "rev-parse", "HEAD"),
        "git_dirty": bool(git_status) if git_status is not None else None,
    }


def reproducibility_fields(
    cfg: Settings,
    data_sha256: str,
    swap_sha256: str | None,
    run_parameters: dict,
    git: dict | None = None,
) -> dict:
    """Identity covers inputs, code, runtime and how the run was made (mode, folds, ...)."""
    code_sha256 = source_sha256()
    runtime = runtime_versions()
    git = git_state() if git is None else git
    identity = ":".join(
        [
            data_sha256,
            cfg.fingerprint,
            swap_sha256 or "no-swap",
            code_sha256,
            json.dumps(run_parameters, sort_keys=True),
            json.dumps(runtime, sort_keys=True),
        ]
    )
    return {
        "experiment_id": hashlib.sha256(identity.encode()).hexdigest(),
        "run_parameters": run_parameters,
        "runtime": runtime,
        "code_sha256": code_sha256,
        **git,
    }

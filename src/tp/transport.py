from pathlib import Path

from tp.config import Settings


def mirror_upstream_raw(settings: Settings) -> None:
    """Copy new/changed files from upstream_raw_dir into raw_remote_dir."""
    pass


def pull_raw(settings: Settings) -> None:
    """Sync raw_remote_dir/current/ -> local raw_dir."""
    pass


def push_processed(settings: Settings, paths: list[Path]) -> None:
    """Push given local paths to the remote processed store."""
    pass

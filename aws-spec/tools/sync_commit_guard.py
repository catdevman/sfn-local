#!/usr/bin/env python3
"""Refuse a sync commit that leaves the generated snapshot paths.

The bot may commit aws-spec/**, internal/api/gen/**, and internal/asl/specgen/**.
It refuses every other path. Handlers, docs/spec.md, surface.toml, and catalog.toml
are named here so a widened prefix cannot sweep them in.
"""

from __future__ import annotations

import argparse
import subprocess
import sys

ALLOW_PREFIXES = (
    "aws-spec/",
    "internal/api/gen/",
    "internal/asl/specgen/",
)


def refusal(path: str) -> str | None:
    path = path.replace("\\", "/").strip()
    if path == "" or path.endswith("/"):
        return f"Refusing to commit {path or '(empty)'}"
    if path == "docs/spec.md":
        return "Refusing to edit docs/spec.md"
    if path == "internal/api/surface.toml" or path.endswith("/surface.toml"):
        return f"Refusing to edit surface.toml ({path})"
    if path == "internal/asl/catalog.toml" or path.endswith("/catalog.toml"):
        return f"Refusing to edit catalog.toml ({path})"
    if path.startswith("internal/api/") and not path.startswith("internal/api/gen/"):
        return f"Refusing to edit handler path {path}"
    if path.startswith("internal/asl/") and not path.startswith("internal/asl/specgen/"):
        return f"Refusing to edit {path}"
    for prefix in ALLOW_PREFIXES:
        if path.startswith(prefix):
            return None
    return f"Refusing to commit {path}"


def porcelain_paths() -> list[str]:
    out = subprocess.check_output(["git", "status", "--porcelain"], text=True)
    paths: list[str] = []
    for line in out.splitlines():
        if len(line) < 4:
            continue
        rest = line[3:].strip()
        if " -> " in rest:
            rest = rest.split(" -> ", 1)[1].strip()
        paths.append(unquote(rest))
    return paths


def cached_paths() -> list[str]:
    out = subprocess.check_output(["git", "diff", "--cached", "--name-only"], text=True)
    return [unquote(line.strip()) for line in out.splitlines() if line.strip()]


def unquote(path: str) -> str:
    if len(path) >= 2 and path[0] == '"' and path[-1] == '"':
        return path[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return path


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--porcelain", action="store_true")
    parser.add_argument("--cached", action="store_true")
    args = parser.parse_args(argv)
    paths = list(args.path)
    if args.porcelain:
        paths.extend(porcelain_paths())
    if args.cached:
        paths.extend(cached_paths())
    bad = [reason for path in paths if (reason := refusal(path))]
    if bad:
        print("\n".join(bad), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

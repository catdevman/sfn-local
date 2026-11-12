#!/usr/bin/env python3
"""List new operations, members, ASL fields, and error names in a generated diff."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any


SURFACE = "internal/api/gen/surface.gen.json"
MATRIX = "internal/asl/specgen/matrix.json"


def load_path(path: str | None) -> Any:
    if not path:
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def load_git(path: str) -> Any:
    result = subprocess.run(["git", "show", f"HEAD:{path}"], capture_output=True)
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def surface_sets(doc: Any) -> tuple[set[str], set[str], set[str]]:
    ops: set[str] = set()
    members: set[str] = set()
    errors: set[str] = set()
    if not doc:
        return ops, members, errors
    for op in doc.get("operations") or []:
        name = op["name"]
        ops.add(name)
        for member in op.get("inputMembers") or []:
            members.add(f"{name} input {member['name']}")
        for member in op.get("outputMembers") or []:
            members.add(f"{name} output {member['name']}")
    for err in doc.get("errors") or []:
        errors.add(err["name"])
    return ops, members, errors


def field_set(doc: Any) -> set[str]:
    fields: set[str] = set()
    if not doc:
        return fields
    for lang in doc.get("languages") or []:
        for field in lang.get("fields") or []:
            fields.add(f"{lang['name']} {field['name']}")
    return fields


def section(title: str, items: set[str]) -> str:
    lines = [f"### {title}", ""]
    if not items:
        lines.append("None.")
    else:
        lines.extend(f"- {item}" for item in sorted(items))
    lines.append("")
    return "\n".join(lines)


def render(old_surface: Any, new_surface: Any, old_matrix: Any, new_matrix: Any) -> str:
    old_ops, old_members, old_errors = surface_sets(old_surface)
    new_ops, new_members, new_errors = surface_sets(new_surface)
    parts = [
        "## Generated surface",
        "",
        section("New operations", new_ops - old_ops),
        section("New members", new_members - old_members),
        section("New ASL fields", field_set(new_matrix) - field_set(old_matrix)),
        section("New error names", new_errors - old_errors),
    ]
    return "\n".join(parts)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--old-surface")
    parser.add_argument("--new-surface")
    parser.add_argument("--old-matrix")
    parser.add_argument("--new-matrix")
    args = parser.parse_args(argv)
    if args.from_git:
        old_surface = load_git(SURFACE)
        new_surface = load_path(args.new_surface or SURFACE)
        old_matrix = load_git(MATRIX)
        new_matrix = load_path(args.new_matrix or MATRIX)
    else:
        old_surface = load_path(args.old_surface)
        new_surface = load_path(args.new_surface)
        old_matrix = load_path(args.old_matrix)
        new_matrix = load_path(args.new_matrix)
    sys.stdout.write(render(old_surface, new_surface, old_matrix, new_matrix))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

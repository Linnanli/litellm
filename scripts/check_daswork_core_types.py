"""Compare OSS wiring diagnostics against the approved source snapshot."""

import argparse
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Final, Literal

from check_daswork_imports import BASE, core_paths
from pydantic import BaseModel, ConfigDict, TypeAdapter
from ruff_strict_gate import parse_changed_lines
from type_check_gate import _temp_worktree, node_options_with_heap

_ROOT: Final = Path(__file__).resolve().parent.parent
_CONFIGS: Final = ("pyrightconfig.json", "pyrightconfig.daswork.json")


class Position(BaseModel):
    model_config = ConfigDict(frozen=True)
    line: int


class DiagnosticRange(BaseModel):
    model_config = ConfigDict(frozen=True)
    start: Position
    end: Position


class Diagnostic(BaseModel):
    model_config = ConfigDict(frozen=True)
    severity: Literal["error", "warning", "information"]
    file: Path
    range: DiagnosticRange
    message: str
    rule: str = "<uncoded>"


class AnalysisSummary(BaseModel):
    model_config = ConfigDict(frozen=True)
    filesAnalyzed: int
    errorCount: int


class Analysis(BaseModel):
    model_config = ConfigDict(frozen=True)
    generalDiagnostics: tuple[Diagnostic, ...]
    summary: AnalysisSummary


_OUTPUT: Final = TypeAdapter(Analysis)


def analyze(root: Path, python: Path, paths: tuple[Path, ...]) -> tuple[Diagnostic, ...]:
    if not paths:
        return ()
    result: Final = subprocess.run(
        ["basedpyright", "--outputjson", "--project", _CONFIGS[1], "--pythonpath", str(python), *map(str, paths)],
        cwd=root,
        env={**os.environ, "NODE_OPTIONS": node_options_with_heap(os.environ)},
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        raise SystemExit(f"basedpyright exited {result.returncode}: {result.stderr}")
    analysis: Final = _OUTPUT.validate_json(result.stdout)
    errors: Final = tuple(item for item in analysis.generalDiagnostics if item.severity == "error")
    if analysis.summary.filesAnalyzed != len(paths) or analysis.summary.errorCount != len(errors):
        raise SystemExit(f"Incomplete basedpyright analysis in {root}")
    return errors


def signature(diagnostic: Diagnostic, root: Path) -> tuple[str, str, str]:
    return (diagnostic.file.resolve().relative_to(root).as_posix(), diagnostic.rule, diagnostic.message)


def main() -> int:
    parser: Final = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pythonpath", type=Path, required=True)
    args: Final = parser.parse_args()
    python: Final = _ROOT / Path(args.pythonpath)
    paths: Final = core_paths()
    if not paths:
        return 0
    diff: Final = subprocess.check_output(
        ["git", "diff", BASE, "--unified=0", "--no-color", "--", *map(str, paths)], cwd=_ROOT, text=True
    )
    changed: Final = parse_changed_lines(diff)
    head: Final = analyze(_ROOT, python, paths)
    with _temp_worktree(BASE) as base:
        for config in _CONFIGS:
            shutil.copyfile(_ROOT / config, base / config)
        baseline: Final = analyze(base, python, tuple(path for path in paths if (base / path).is_file()))
        base_root: Final = base.resolve()
        baseline_counts: Final = Counter(signature(item, base_root) for item in baseline)
    added: Final = Counter(signature(item, _ROOT) for item in head) - baseline_counts
    edited: Final = tuple(
        item
        for item in head
        if changed.get(item.file.relative_to(_ROOT).as_posix(), set()).intersection(
            range(item.range.start.line + 1, item.range.end.line + 2)
        )
    )
    for (path, rule, message), count in sorted(added.items()):
        sys.stdout.write(f"{path}: {count} new {rule}: {message}\n")
    for item in edited:
        sys.stdout.write(f"{item.file.relative_to(_ROOT)}:{item.range.start.line + 1}: {item.rule} on an edited line\n")
    sys.stdout.write(
        f"OSS types against {BASE}: {len(baseline)} base, {len(head)} current, {sum(added.values())} new\n"
    )
    return int(bool(added or edited))


if __name__ == "__main__":
    raise SystemExit(main())

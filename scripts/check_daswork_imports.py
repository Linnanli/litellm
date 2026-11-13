"""Reject proprietary imports in new code and product entry-point wiring."""

import ast
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Final

BASE: Final = "b2bae43fdefdeab4ac7aae08dba402dbd9dddff7"


def core_paths() -> tuple[Path, ...]:
    result: Final = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMR", BASE, "--", "litellm", "litellm_cli"],
        check=True,
        capture_output=True,
        text=True,
    )
    return tuple(Path(line) for line in result.stdout.splitlines() if line.endswith(".py"))


def forbidden(module: str) -> bool:
    return module.partition(".")[0] in {"enterprise", "litellm_enterprise"} or module.startswith(
        "litellm.proxy.enterprise"
    )


def imports(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.ImportFrom):
        return (node.module or "",)
    if isinstance(node, ast.Import):
        return tuple(alias.name for alias in node.names)
    if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
        function: Final = node.func
        if (isinstance(function, ast.Name) and function.id in {"__import__", "import_module"}) or (
            isinstance(function, ast.Attribute) and function.attr == "import_module"
        ):
            value: Final = node.args[0].value
            return (value,) if isinstance(value, str) else ()
    return ()


def main(args: tuple[str, ...]) -> int:
    if args == ("--core-paths",):
        sys.stdout.write("".join(f"{path}\n" for path in core_paths()))
        return 0
    package: Final = Path("daswork_enterprise")
    paths: Final = (*package.glob("*.py"), *(package / "tests").rglob("*.py"))
    violations: Final = (
        *(path for path in paths if prohibited_imports(path.read_text(encoding="utf-8"), path)),
        *(path for path in core_paths() if has_new_prohibited_imports(path)),
    )
    for path in violations:
        sys.stderr.write(f"{path}: prohibited proprietary import\n")
    return int(bool(violations))


def prohibited_imports(source: str, path: Path) -> Counter[str]:
    nodes: Final = ast.walk(ast.parse(source, filename=str(path)))
    return Counter(module for node in nodes for module in imports(node) if forbidden(module))


def has_new_prohibited_imports(path: Path) -> bool:
    prior: Final = subprocess.run(
        ["git", "show", f"{BASE}:{path.as_posix()}"],
        capture_output=True,
        text=True,
    )
    current: Final = prohibited_imports(path.read_text(encoding="utf-8"), path)
    baseline: Final = prohibited_imports(prior.stdout, path) if prior.returncode == 0 else Counter[str]()
    return bool(current - baseline)


if __name__ == "__main__":
    raise SystemExit(main(tuple(sys.argv[1:])))

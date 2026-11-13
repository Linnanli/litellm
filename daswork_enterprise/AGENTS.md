# Daswork gateway

The approved clean-room snapshot and reviewed behavior handoff are the implementation inputs. The comparison base for source checks is the independent root commit `b2bae43fdefdeab4ac7aae08dba402dbd9dddff7`

Java owns employee, organization and session lifecycle. This package owns gateway admission and request accounting. Keep a single serving process, a single PostgreSQL writer and one authoritative settlement for each gateway call. Keep the scope captured at admission until settlement

Validate external bodies, deployment settings and database records at their boundaries. Keep network calls outside budget locks. Log sanitized failure categories, checkpoints and call IDs. Keep dependencies and provider calls synthetic in tests

Run `make check-daswork` and the real PostgreSQL and Redis tests before release. Keep shared OSS code changes limited to wiring. Document each rule suppression beside its use

`make check-daswork` requires zero type errors in this package. The core check runs the same basedpyright version, configuration and locked environment against the changed OSS files and the approved root. A new diagnostic or a diagnostic on an edited line fails CI. To run only the core comparison, use `uv run --frozen --extra proxy --extra extra_proxy --extra proxy-runtime python scripts/check_daswork_core_types.py --pythonpath .venv/bin/python`

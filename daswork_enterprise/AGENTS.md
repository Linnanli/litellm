# Daswork gateway

The approved clean-room snapshot and reviewed behavior handoff are the implementation inputs. The comparison base for source checks is the independent root commit `b2bae43fdefdeab4ac7aae08dba402dbd9dddff7`

Java owns employee, organization and session lifecycle. This package owns gateway admission and request accounting. Keep a single serving process, a single PostgreSQL writer and one authoritative settlement for each gateway call. Keep the scope captured at admission until settlement

Validate external bodies, deployment settings and database records at their boundaries. Keep network calls outside budget locks. Log sanitized failure categories, checkpoints and call IDs. Keep dependencies and provider calls synthetic in tests

Run `make check-daswork` and the real PostgreSQL and Redis tests before release. Keep shared OSS code changes limited to wiring. Document each rule suppression beside its use

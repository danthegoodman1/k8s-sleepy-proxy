# One-request stateless re-wake

The re-wake check now issues exactly one HTTP GET after authoritative Cold,
generation advancement, and observed Kubernetes cleanup. The existing 11-second
cache-expiry wait remains. The request has the same 140-second outer deadline
as initial cold delivery. Both cold requests use the known-length response
reader, with a 130-second blocking response deadline; no reader waits for EOF or
retains the previous 180-second socket budget. HTTP errors, non-200 responses,
and incorrect app bodies are fatal.

The former HTTP convergence/retry helper is removed. Read-only state and
Kubernetes convergence remain, as do the exact ownership, bounded hot-cache,
and idle checks. The fixture has no application-failure retry path. The hot
traffic loop still allows another request only after the prior one succeeded.

[Focused fixture tests](stateless-one-shot-rewake-tests.log) completed exit 0:
five passed, with the deployed kind test ignored. The new
`one_shot_application_error_is_fatal_without_waiting_for_eof` regression observes
one actual GET, sends a complete 502 while keeping the response socket open,
requires the client to return it promptly, rejects it via the same assertion as
re-wake, and verifies no second connection was made. The
[first local attempt](stateless-one-shot-rewake-fixture-failure.log) exposed a
test-server assumption that one TCP read contains a complete request line; that
server now collects bounded headers before checking them.

[Scoped strict clippy](stateless-one-shot-rewake-clippy.log), scoped rustfmt,
and diff checks completed exit 0. No production code or script changed and no
kind operation was run for this packet. The earlier 54.05-second deployed pass
used the old retrying re-wake check and does not count as evidence for this
stronger check. The coordinator's separate 502 investigation and subsequent
deployed rerun remain required.

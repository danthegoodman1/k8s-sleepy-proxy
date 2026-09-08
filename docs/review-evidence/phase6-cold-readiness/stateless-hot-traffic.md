# Stateless hot-cache observation

The coordinator's `20260908T010413Z-stateless.log` reached the working dedicated
exporter but failed after 30 seconds with Subscribe successes 23 and cache hits
zero. The fixture sent one post-cold request, then only scraped metrics; its class
idles after two seconds. A late wake notification may invalidate that first
lookup, and metrics-only polling cannot create a later cache hit. The cumulative
Subscribe count includes cold-resolution attempts and does not itself prove
continuous background cache churn.

The fixture now observes a new hit under bounded healthy traffic. It captures
the initial hit counter and sends individual read-only GETs, checking each HTTP
200 and app marker. Each request has a one-second outer deadline and a 900 ms
underlying socket timeout. Any request error, wrong status, or wrong body is
fatal. After a successful request it scrapes the dedicated exporter and requires
the hit counter to increase, plus at least one successful Subscribe. Only
successful requests may be followed by another request, after 100 ms. The entire
hot phase has a five-second deadline, below the ten-second positive cache TTL.
The initial cold request remains one request with its existing deadline.

The first bounded attempt exposed another fixture framing issue:
[the one-second timeout](stateless-hot-timeout-failure.log) hid its underlying
read diagnostic. Shortening the socket wait to 900 ms retained
[the complete response](stateless-hot-framing-failure.log): HTTP 200, advertised
Content-Length 25, and all 234 bytes received (209 header/delimiter bytes plus
25 body bytes), followed by OS 35 while awaiting EOF. Hot BusyBox responses now
use the same bounded Content-Length reader as the exporter. No application
response failure was retried or counted as a pass. No production code changed.

## Completed evidence

- [Actual isolated stateless run](stateless-hot-fixed.log): exit 0, one deployed
  test passed in 54.05 seconds on the already-loaded final 6I images, with no
  build, pull, or load. Hot request one succeeded with Subscribe successes 30 /
  hits zero at 10.83 ms. Hot request two succeeded with the same Subscribe count
  and hits one at 120.25 ms. This proves stable traffic produces a cache hit
  well before TTL expiry. The run also passed initial one-shot cold delivery,
  exact layout/ownership, idle cleanup, and the existing re-wake check.
- [Retained image identities](stateless-hot-images.log) match the coordinator's
  frozen images: control plane `ef1f6d805759...`, frontline `af67717ec9e4...`.
- [Four framing/metric tests](stateless-hot-local-tests.log) and
  [scoped strict clippy](stateless-hot-clippy.log): completed exit 0.
  Scoped rustfmt, Bash syntax, and diff checks also completed exit 0.

The diagnosis namespaces were retained and the cluster was released. A separate
[single-request diagnostic re-wake](stateless-rewake-diagnostic-failure.log)
returned 502 after 5.65 seconds and stopped immediately. Its durable timestamps
show wake completion followed by sleep intent before that response, but do not
establish the exact transport failure cause. The coordinator assigned that
finding to a separate investigation. The passing fixture's existing re-wake
helper permits convergence retries, so it does not supersede that one-request
failure or prove a first-request re-wake guarantee.

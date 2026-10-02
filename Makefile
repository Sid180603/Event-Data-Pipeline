# T-R5: the demo's single entry point.
#
# WHY this file exists: the demo is run from a fresh clone by somebody who has
# not read the runbook, on a machine where Docker lives in WSL2 and the tools
# live on the host. A README with four commands in it is four chances to get the
# order wrong. These targets ARE the order.
#
#   make demo      up -> load run -> live view -> verify   (the default goal)
#   make up        the compose stack, and wait until the gateway is healthy
#   make observe   the live events/sec + error rate view (tools/observe.py)
#   make verify    reconcile the ledger against what the broker stored
#   make chaos     the four chaos beats, in order
#   make test      the whole suite, serially
#   make inspect   one event: what the client sent | what is stored | what
#                  the operator endpoint hands back (app/inspect.py)
#   make help      this list
#
# `make demo` from a clean checkout needs WSL2 + Docker, and the `.env` +
# `driver-signing-key.pem` that the header of docker-compose.yml generates.
# It needs NOTHING published to the host: every step below runs inside the
# compose network, so the gateway's listener -- plaintext, carrying bearer
# tokens and plaintext PII -- is never bound to a host interface at all. To read
# the live view from your own shell instead, uncomment the
# `127.0.0.1:8000:8000` mapping in docker-compose.yml and add `IN_NETWORK=0`.
# Never 0.0.0.0: that listener must not be reachable from the LAN.
#
# Every recipe runs under `set -e -u -o pipefail` and nothing here pipes a
# failure into a `|| true`. A demo target that swallows a failure is a demo that
# lies, which is the one thing this whole slice exists to avoid.
#
# NOT WRITTEN YET: `make verify` and `make chaos` point at tools/verify.py and
# scripts/chaos/*.sh, which belong to another task (R3/R4) and are not in the
# tree yet. Those two targets fail loudly until those land; that is intended.

.DEFAULT_GOAL := demo

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE ?= docker compose
PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest -q -p no:cacheprovider
PYTHON_RAW ?= $(PYTHON) -m

# From inside the gateway container, the gateway IS localhost. From the host it
# is 127.0.0.1:8000, and only because compose's mapping binds that address.
GATEWAY_URL ?= http://localhost:8000
HEALTH_URL ?= $(GATEWAY_URL)/healthz
METRICS_URL ?= $(GATEWAY_URL)/metrics

# Where the driver's own default points (driver/main.py: DEFAULT_LEDGER), which
# is inside the bind mount, so both the driver and the verifier see the file.
LEDGER ?= ledger.jsonl
DRIVER_LOG ?= driver-run.log

# 1 = run the tools inside the compose network (default, nothing published).
# 0 = run them on the host against a published 127.0.0.1 port.
IN_NETWORK ?= 1

# The live view is a foreground screen and the load run is what feeds it, so
# `demo` overlaps the two: a view that starts after the run finishes reports
# zero events/sec, which is true and useless. These are its defaults; override
# with OBSERVE_ARGS=... .
OBSERVE_ARGS ?= --interval 1
DEMO_SAMPLES ?= --samples 8 --interval 1

# `-T` on every compose call: these recipes are non-interactive and a forced
# TTY on a redirected stream is a hang, not a nicety.
DRIVER = $(COMPOSE) run --rm -T driver

ifeq ($(IN_NETWORK),1)
OBSERVE = $(COMPOSE) exec -T gateway $(PYTHON_RAW) tools.observe --url $(METRICS_URL) $(OBSERVE_ARGS)
VERIFY = $(COMPOSE) exec -T gateway $(PYTHON_RAW) tools.verify $(VERIFY_ARGS)
INSPECT = $(COMPOSE) exec -T gateway $(PYTHON_RAW) app.inspect $(INSPECT_ARGS)
else
OBSERVE = $(PYTHON_RAW) tools.observe --url $(METRICS_URL) $(OBSERVE_ARGS)
VERIFY = $(PYTHON_RAW) tools.verify $(VERIFY_ARGS)
INSPECT = $(PYTHON_RAW) app.inspect $(INSPECT_ARGS)
endif

.PHONY: help up preflight wait down logs driver demo observe verify inspect chaos test _ledger

help:
	@echo "make demo      up -> load run -> live view -> verify   (default goal)"
	@echo "make up        the compose stack, and wait for the gateway to be healthy"
	@echo "make observe   live events/sec + error rate     (OBSERVE_ARGS=..., IN_NETWORK=0|1)"
	@echo "make verify    reconcile the ledger against the broker   (VERIFY_ARGS=...)"
	@echo "make inspect   one event, sent | stored | decrypted      (INSPECT_ARGS=...)"
	@echo "make chaos     kill-gateway, broker-down, bad-events, tenant-flood, in order"
	@echo "make test      python -m pytest -q -p no:cacheprovider"
	@echo "make down      stop the stack and drop its volumes"
	@echo "make logs      follow the gateway's log"
	@echo ""
	@echo "tools run inside the compose network by default. Add IN_NETWORK=0 to run them"
	@echo "on the host, which needs the 127.0.0.1:8000 mapping in docker-compose.yml."

up:
	@$(MAKE) --no-print-directory preflight
	$(COMPOSE) up -d
	@$(MAKE) --no-print-directory wait

# Compose would fail on a missing `env_file`, but with a message about a file it
# expects rather than about the two the operator has to generate. The generator
# is the python snippet in the header of docker-compose.yml; it is not copied
# here because a second copy of a secret generator is a second thing to keep
# honest.
preflight:
	@test -f .env || { \
		echo "no .env: the gateway refuses to start without MASTER_SECRET and a JWT" >&2; \
		echo "public key, and compose will not invent them. Run the generator in the" >&2; \
		echo "header of docker-compose.yml, from the repository root." >&2; \
		exit 1; \
	}
	@test -f driver-signing-key.pem || { \
		echo "no driver-signing-key.pem: that is the PRIVATE half of the signing pair," >&2; \
		echo "which the same generator writes. The gateway must never be able to read it." >&2; \
		exit 1; \
	}

# The gateway's own healthcheck has a 90s start_period, so "up -d" returning is
# not "the gateway is serving". This polls /healthz until it answers, and prints
# the log if it never does -- which is the difference between a 30-second pause
# and a demo that fails on a gateway that is merely still starting.
wait:
	@echo "waiting for the gateway to answer $(HEALTH_URL) ..."
	@for attempt in $$(seq 1 60); do \
		if $(COMPOSE) exec -T gateway $(PYTHON) -c \
			"import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('$(HEALTH_URL)', timeout=3).status == 200 else 1)" \
			>/dev/null 2>&1; then \
			echo "gateway healthy"; exit 0; \
		fi; \
		echo "  not answering yet ($${attempt}/60) ..." >&2; \
		sleep 10; \
	done; \
	echo "the gateway did not become healthy within 600s. Last 40 lines:" >&2; \
	$(COMPOSE) logs --tail 40 gateway >&2; \
	exit 1

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f gateway

# A load run on its own. The ledger check is the belt to `compose run`'s braces:
# a driver that lost every batch still writes a ledger, and a driver that never
# ran writes none, so an empty file here means the run did not happen at all.
# A prerequisite would run it BEFORE the load; the check is the recipe's last
# step for that reason.
driver:
	$(DRIVER)
	@$(MAKE) --no-print-directory _ledger

_ledger:
	@test -s $(LEDGER) || { \
		echo "no $(LEDGER): the driver wrote nothing, so the load run did not happen." >&2; \
		echo "its output, had there been any, would be in $(DRIVER_LOG) for a foreground run." >&2; \
		exit 1; \
	}

# The end-to-end story, in the order it has to happen. Sequential `$(MAKE)`
# calls rather than prerequisites: with `make -j` prerequisites interleave, and
# a live view that starts before the load run is reporting nothing.
demo:
	@$(MAKE) --no-print-directory up
	@echo ""
	@echo "== load run in the background, live view in the foreground, same window =="
	@$(DRIVER) > $(DRIVER_LOG) 2>&1 & driver_pid=$$!; \
	$(MAKE) --no-print-directory observe OBSERVE_ARGS='$(DEMO_SAMPLES)'; \
	wait $$driver_pid || { \
		echo "" >&2; \
		echo "the load run failed; the driver said:" >&2; \
		cat $(DRIVER_LOG) >&2; \
		exit 1; \
	}; \
	cat $(DRIVER_LOG); \
	echo ""; \
	echo "== the driver's own receipt above; reconciling it against the broker =="; \
	$(MAKE) --no-print-directory _ledger; \
	$(MAKE) --no-print-directory verify

observe:
	@$(OBSERVE)

verify:
	@$(VERIFY)

# Not part of `demo`: it is its own beat, and it runs entirely in-process
# against a FakeSink, so it needs no broker, no topics and no load run.
inspect:
	@$(INSPECT)

# The four beats, in the order the runbook uses. `bash <script>` rather than
# `./<script>` so a script that arrives without its executable bit still runs.
# Run `make verify` afterwards: that is what these are for.
chaos:
	@echo "== chaos, in order: kill-gateway, broker-down, bad-events, tenant-flood =="
	@echo "== (make verify afterwards -- the beats are only interesting once reconciled)"
	bash scripts/chaos/kill_gateway.sh
	bash scripts/chaos/broker_down.sh
	bash scripts/chaos/bad_events.sh
	bash scripts/chaos/tenant_flood.sh

test:
	$(PYTEST)

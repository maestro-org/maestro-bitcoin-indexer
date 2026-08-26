.PHONY: build test lint fmt up down logs multi gc swagger playground-up playground-run playground-down hygiene

# Build everything (workspace + standalone compressor-xbt)
build:
	cargo build --workspace
	cd services/compressor-xbt && cargo build

test:
	cargo test --workspace
	cd services/compressor-xbt && cargo test

lint:
	cargo clippy --workspace --all-targets
	cd services/compressor-xbt && cargo clippy --all-targets

fmt:
	cargo fmt --all
	cd services/compressor-xbt && cargo fmt

# Docker compose stack (testnet4)
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail 100

# Start the second polyphony instance (parallel backfill / swap-over demo)
multi:
	docker compose --profile multi up -d --build polyphony-b

# Manual one-shot TiKV MVCC garbage collection (the tikv-gc service also runs
# automatically every 10 minutes)
gc:
	docker compose run --rm tikv-gc tikv-gc

# Playground: native services against tiup TiKV (see scripts/playground.sh)
playground-up:
	scripts/playground.sh up

playground-run:
	scripts/playground.sh run

playground-down:
	scripts/playground.sh down

# Regenerate mapi-xbt's OpenAPI documents (run from services/mapi-xbt, writes docs/*/swagger.json)
swagger:
	cd services/mapi-xbt && \
	cargo run --release -p mapi-xbt -- --mode generate-open-api && \
	cargo run --release -p mapi-xbt -- --mode generate-open-api-mempool && \
	cargo run --release -p mapi-xbt -- --mode generate-open-api-wallet

# Check for internal references that must not ship
hygiene:
	@! grep -rniE "gomaestro|pkg\.dev|svc\.cluster\.local|maestro-org-development|ssh://|DEPLOY_KEY|haproxy-dataplane|dotswap|magic.?eden" \
		--include="*.rs" --include="*.toml" --include="*.md" --include="*.yml" --include="*.yaml" --include="Dockerfile" --include="*.sh" \
		--exclude-dir=target . \
		| grep -v "^./Makefile" || (echo "hygiene check failed" && exit 1)
	@echo "hygiene check passed"

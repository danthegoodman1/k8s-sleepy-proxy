.PHONY: test infra-up images-push deploy seed-tenant seed-postgres-tenant demo demo-archil demo-postgres destroy

test:
	go test ./...

infra-up:
	./scripts/infra-up.sh

images-push:
	./scripts/images-push.sh

deploy:
	./scripts/deploy.sh

seed-tenant:
	./scripts/seed-tenant.sh

seed-postgres-tenant:
	./scripts/seed-postgres-tenant.sh

demo:
	./scripts/demo.sh

demo-archil:
	./scripts/demo-archil.sh

demo-postgres:
	./scripts/demo-postgres.sh

destroy:
	./scripts/destroy.sh

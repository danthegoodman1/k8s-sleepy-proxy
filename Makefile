.PHONY: test infra-up images-push deploy seed-tenant demo demo-archil destroy

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

demo:
	./scripts/demo.sh

demo-archil:
	./scripts/demo-archil.sh

destroy:
	./scripts/destroy.sh

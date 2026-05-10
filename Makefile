.PHONY: test infra-up images-push deploy seed-tenant demo destroy

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

destroy:
	./scripts/destroy.sh

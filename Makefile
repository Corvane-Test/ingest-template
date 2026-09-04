CLUSTER        ?= ingest-lab
INGEST_IMAGE   ?= ingest:latest

.PHONY: up build load deploy logs grafana down

up: build
	k3d cluster create $(CLUSTER) \
		-p "8080:30080@loadbalancer" \
		-p "3000:30000@loadbalancer" \
		-p "8081:30081@loadbalancer"
	k3d image import $(INGEST_IMAGE) -c $(CLUSTER)
	kubectl create configmap grafana-dashboard-ingest \
		--from-file=dashboards/ingest-overview.json --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f k8s/
	kubectl wait --for=condition=Available deployment --all --timeout=600s
	kubectl wait --for=condition=Ready pod --all --timeout=600s
	@echo "ingest:   http://localhost:8080"
	@echo "grafana:  http://localhost:3000"
	@echo "upstream: http://localhost:8081/stats"

build:
	docker build -t $(INGEST_IMAGE) .

load:
	python3 tools/load.py

# fast redeploy
deploy: build
	k3d image import $(INGEST_IMAGE) -c $(CLUSTER)
	kubectl delete pod ingest --grace-period=0 --force
	kubectl apply -f k8s/ingest-pod.yaml

logs:
	kubectl logs -f -l app=ingest

grafana:
	@echo "http://localhost:3000"

down:
	k3d cluster delete $(CLUSTER)

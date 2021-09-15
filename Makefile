timescale:
	docker-compose up -d timescaledb

kafka:
	docker-compose up -d zookeeper kafka

check-topic:
	kafkacat -b localhost:29092 -t queueing.transactions -C -O -o-1 | jq .

run-producer:
	docker-compose up producer

down:
	docker-compose down -v
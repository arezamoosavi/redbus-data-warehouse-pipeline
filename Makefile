timescale:
	docker-compose up -d timescaledb

kafka:
	docker-compose up -d zookeeper kafka

producer:
	docker-compose up producer

pie_consumer:
	docker-compose up pie_consumer

down:
	docker-compose down -v
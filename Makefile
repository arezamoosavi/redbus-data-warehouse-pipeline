timescaledb:
	docker-compose up -d timescaledb-data1 timescaledb-data2 timescaledb

roachdb:
	docker-compose up -d roach1 roach2 roach3
kafka:
	docker-compose up -d zookeeper kafka

producer:
	docker-compose up producer

pie_consumer:
	docker-compose up pie_consumer

down:
	docker-compose down -v
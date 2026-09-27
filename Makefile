.PHONY: up down env

env:
	cp .env.example .env

up:
	docker-compose up -d
	@echo "Đang đợi các dịch vụ khởi động (khoảng 15s)..."
	@sleep 15
	@echo "================================================="
	@echo "Các dịch vụ đã sẵn sàng:"
	@echo "- Kafka UI:        http://localhost:8080"
	@echo "- Spark Master UI: http://localhost:8082"
	@echo "- MinIO Console:   http://localhost:9001"
	@echo "- PostgreSQL:      localhost:5432 (aq_user/aq_password)"
	@echo "================================================="

down:
	docker-compose down -v

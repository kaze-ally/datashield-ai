# kafka/topics.append.sh (Week 4)
#
# Append to your real kafka/topics.sh, alongside the drift-events line from
# Week 3.

docker compose exec -T kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic remediation-requests \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

docker compose exec -T kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic remediation-dlq \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Debezium Kafka Connect worker.
#
# NOTE: this is Kafka Connect (Debezium), NOT the Helm chart named "connect" on
# charts.redpanda.com -- that one is Redpanda Connect (Benthos), a different
# product entirely.
#
# M1 builds the base only, so `make build && make load && make reload` is a real
# exercised path. M3 (DATA-704) adds the Confluent Avro converter jars here:
# Debezium 2.0+ removed native Confluent Avro converter support, and Apicurio's
# converter speaks Apicurio's REST API so it cannot talk to Redpanda's registry.
# Redpanda's registry is Confluent-API-compatible, so Confluent's converter is
# the path that matches the requirement. That jar is Confluent Community License,
# not Apache 2.0 -- fine for local and internal use, worth knowing before this
# goes anywhere shared.
FROM quay.io/debezium/connect:3.0.0.Final

# M3: COPY the confluent avro converter + its transitive deps into
# $KAFKA_CONNECT_PLUGINS_DIR/confluent-avro/ here.

LABEL org.opencontainers.image.title="pit-connect" \
      org.opencontainers.image.description="Debezium Connect worker for the PIT PoC"

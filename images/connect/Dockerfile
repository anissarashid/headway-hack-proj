# Kafka Connect + Debezium Postgres, with the Confluent Avro converter added.
#
# NOTE: this is Kafka Connect (Debezium), NOT the Helm chart named "connect" on
# charts.redpanda.com -- that one is Redpanda Connect (Benthos), a different
# product entirely.
#
# Debezium 2.0+ removed the bundled Confluent Avro converter. Redpanda's registry
# is Confluent-API-compatible, so io.confluent:kafka-connect-avro-converter is the
# converter that fits (Apicurio's speaks a different REST API and cannot talk to
# Redpanda's registry). That jar is Confluent Community License, not Apache 2.0 --
# fine for local and internal use, worth knowing before this goes anywhere shared.
# We resolve it (and its transitive deps) with Maven in a build stage and drop the
# jars into a plugin directory the Connect image already scans.

# ---- Stage 1: resolve the converter + all transitive dependencies ----------
FROM maven:3.9-eclipse-temurin-17 AS deps
WORKDIR /build
COPY pom.xml .
# copy-dependencies pulls the whole runtime graph into one directory.
RUN mvn -q -B dependency:copy-dependencies \
      -DoutputDirectory=/plugins/avro-converter \
      -DincludeScope=runtime

# ---- Stage 2: Debezium Connect + the resolved converter --------------------
FROM quay.io/debezium/connect:3.0.0.Final

# The Debezium image adds every subdirectory of /kafka/connect to plugin.path,
# so placing the converter jars here makes the converter classes resolvable to
# key.converter / value.converter without touching the worker classpath.
COPY --from=deps /plugins/avro-converter/ /kafka/connect/avro-converter/

LABEL org.opencontainers.image.title="pit-connect" \
      org.opencontainers.image.description="Debezium Connect worker with the Confluent Avro converter for the PIT PoC"

# Kafka Connect + Debezium Postgres, with the Confluent Avro converter added.
#
# Debezium 2.0+ removed the bundled Confluent Avro converter, so we resolve it
# (and its transitive deps) with Maven in a build stage and drop the jars into a
# plugin directory the Connect image already scans.

# ---- Stage 1: resolve the converter + all transitive dependencies ----------
FROM maven:3.9-eclipse-temurin-17 AS deps
WORKDIR /build
COPY pom.xml .
# copy-dependencies pulls the whole runtime graph into one directory.
RUN mvn -q -B dependency:copy-dependencies \
      -DoutputDirectory=/plugins/avro-converter \
      -DincludeScope=runtime

# ---- Stage 2: Debezium Connect + the resolved converter --------------------
FROM quay.io/debezium/connect:3.0

# The Debezium image adds every subdirectory of /kafka/connect to plugin.path,
# so placing the converter jars here makes the converter classes resolvable to
# key.converter / value.converter without touching the worker classpath.
COPY --from=deps /plugins/avro-converter/ /kafka/connect/avro-converter/

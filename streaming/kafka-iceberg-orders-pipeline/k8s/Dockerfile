# Image used by every pod of the pipeline: Debian + Java 17 + Spark + Kafka/Iceberg jars + the app jar + the data files.
# Build it with build-image.ps1 (run from the project root).
FROM debian:bookworm-slim

# Same versions as build.sbt
ARG SPARK_VERSION=3.5.9
ARG ICEBERG_VERSION=1.12.0

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/opt/java \
    PATH=/opt/spark/bin:/opt/java/bin:$PATH

# Java 17, bash and tini (needed by Spark's Kubernetes entrypoint), curl to download Spark
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless bash tini procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

# Spark (tries the current mirror first, then the archive)
RUN (curl -fsSL -o /tmp/spark.tgz https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
     || curl -fsSL -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz) \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
 && rm /tmp/spark.tgz \
 && cp /opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/entrypoint.sh \
 && cp /opt/spark/kubernetes/dockerfiles/spark/decom.sh /opt/decom.sh \
 && chmod +x /opt/entrypoint.sh /opt/decom.sh \
 && mkdir -p /opt/spark/work-dir

# Kafka connector (+ its dependencies) and Iceberg runtime: Spark's own package resolver downloads them,
# then only the jars we need are copied next to Spark's jars.
RUN echo 'sys.exit(0)' | /opt/spark/bin/spark-shell \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION} \
      --conf spark.jars.ivy=/tmp/ivy \
 && cp /tmp/ivy/jars/*kafka* /tmp/ivy/jars/*commons-pool2* /tmp/ivy/jars/*iceberg-spark-runtime* /opt/spark/jars/ \
 && rm -rf /tmp/ivy \
 && ls /opt/spark/jars | grep -E "kafka|commons-pool2|iceberg"

# The application and the micro-batch data files
COPY target/scala-2.12/*.jar /opt/jars/orders-pipeline.jar
COPY data /opt/app/data

# Demo simplification: the container runs as root so the pods can write to the shared volume.
WORKDIR /opt/app
ENTRYPOINT ["/opt/entrypoint.sh"]

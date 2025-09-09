#/*
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *      http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Build stage
FROM maven:3-eclipse-temurin-8 AS builder

WORKDIR /workspace
COPY . .

# Define which source connectors to build
ARG SOURCE_CONNECTORS="mysql postgres"

# Build the specified connectors using a loop
RUN for connector in $SOURCE_CONNECTORS; do \
    CONNECTOR_PATHS="$CONNECTOR_PATHS,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-${connector}-cdc"; \
    done && \
    CONNECTOR_PATHS="${CONNECTOR_PATHS#,}" && \
    mvn clean install -DskipTests -am -pl "$CONNECTOR_PATHS"

# Copy all built JARs to a common target directory
RUN mkdir -p /target && \
    for connector in $SOURCE_CONNECTORS; do \
        cp /workspace/flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-${connector}-cdc/target/*.jar /workspace/target/; \
    done

# Final stage
FROM scratch
# Copy the consolidated JAR files from builder
COPY --from=builder /workspace/target/*.jar /target/

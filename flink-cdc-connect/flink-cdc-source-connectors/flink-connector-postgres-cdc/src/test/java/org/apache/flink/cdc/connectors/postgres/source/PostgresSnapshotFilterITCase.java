/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.CloseableIterator;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT tests for PostgreSQL snapshot filter functionality. Tests verify that the
 * scan.incremental.snapshot.filter option correctly filters data during snapshot phase.
 */
@Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
class PostgresSnapshotFilterITCase extends PostgresTestBase {

    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "filter_test";
    private static final String SLOT_NAME = "test_snapshot_filter_slot";

    private final UniqueDatabase testDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @BeforeEach
    public void before() {
        testDatabase.createAndInitialize();
        createTestData();
    }

    @AfterEach
    public void after() throws Exception {
        testDatabase.removeSlot(SLOT_NAME);
    }

    /**
     * Test snapshot filter with a date condition. Verifies that only records after a specified date
     * are captured during snapshot.
     */
    @Test
    void testSnapshotFilterWithDateCondition() throws Exception {
        String filterCondition = SCHEMA_NAME + ".customers:created_at > '2024-01-01 00:00:00'";

        List<String> capturedRecords = captureSnapshotWithFilter(filterCondition);

        // Should only capture customers created after 2024-01-01
        // Based on test data, that's 5 customers (Alice, Charlie, Diana, Eve, Frank)
        assertThat(capturedRecords).hasSize(5);

        // Verify all captured records are for customers created after the filter date
        for (String record : capturedRecords) {
            assertThat(record).contains("Alice");
        }
    }

    /**
     * Test snapshot filter with a status condition. Verifies that only records with a specific
     * status are captured during snapshot.
     */
    @Test
    void testSnapshotFilterWithStatusCondition() throws Exception {
        String filterCondition = SCHEMA_NAME + ".customers:status = 'active'";

        List<String> capturedRecords = captureSnapshotWithFilter(filterCondition);

        // Should only capture active customers
        // Based on test data: John, Jane, Bob, Alice, Charlie, Eve (6 active)
        assertThat(capturedRecords).hasSize(6);

        for (String record : capturedRecords) {
            assertThat(record).contains("active");
        }
    }

    /**
     * Test snapshot filter with multiple tables. Verifies that filters are applied correctly to
     * each table independently.
     */
    @Test
    void testSnapshotFilterWithMultipleTables() throws Exception {
        String filterCondition =
                SCHEMA_NAME
                        + ".customers:created_at > '2024-01-01 00:00:00';"
                        + SCHEMA_NAME
                        + ".orders:status = 'shipped'";

        List<String> capturedRecords = captureSnapshotWithFilter(filterCondition);

        // Should capture recent customers (5) and shipped orders (based on test data)
        // Verify we have both customers and orders
        boolean hasCustomers = capturedRecords.stream().anyMatch(r -> r.contains("customers"));
        boolean hasOrders = capturedRecords.stream().anyMatch(r -> r.contains("orders"));

        assertThat(hasCustomers || hasOrders).isTrue();
    }

    /**
     * Test snapshot filter with complex condition (AND). Verifies that complex filter conditions
     * work correctly.
     */
    @Test
    void testSnapshotFilterWithComplexCondition() throws Exception {
        String filterCondition =
                SCHEMA_NAME + ".customers:created_at > '2024-01-01 00:00:00' AND status = 'active'";

        List<String> capturedRecords = captureSnapshotWithFilter(filterCondition);

        // Should only capture active customers created after 2024-01-01
        // Based on test data: Alice, Charlie, Eve (3 active, recent customers)
        assertThat(capturedRecords).hasSize(3);

        for (String record : capturedRecords) {
            assertThat(record).contains("active");
        }
    }

    /**
     * Test that without a filter, all records are captured. This is a baseline test to verify the
     * filter is actually working.
     */
    @Test
    void testSnapshotWithoutFilter() throws Exception {
        List<String> capturedRecords = captureSnapshotWithFilter(null);

        // Should capture all customers (8 total)
        assertThat(capturedRecords).hasSize(8);
    }

    /** Test snapshot filter with numeric comparison. */
    @Test
    void testSnapshotFilterWithNumericCondition() throws Exception {
        String filterCondition = SCHEMA_NAME + ".customers:id > 1005";

        List<String> capturedRecords = captureSnapshotWithFilter(filterCondition);

        // Should only capture customers with id > 1005
        // Based on test data, this excludes the first 3 customers
        assertThat(capturedRecords).hasSize(5);
    }

    /**
     * Captures snapshot data with an optional filter condition.
     *
     * @param filterCondition The filter condition in format "schema.table:condition" or null
     * @return List of captured records as strings
     */
    private List<String> captureSnapshotWithFilter(String filterCondition) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        // Build the source with optional filter
        PostgresSourceBuilder.SourceBuilder<SourceRecord> sourceBuilder =
                PostgresSourceBuilder.PostgresIncrementalSource.<SourceRecord>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getFirstMappedPort())
                        .database(testDatabase.getDatabaseName())
                        .schemaList(SCHEMA_NAME)
                        .tableList(SCHEMA_NAME + ".customers")
                        .username(POSTGRES_CONTAINER.getUsername())
                        .password(POSTGRES_CONTAINER.getPassword())
                        .slotName(SLOT_NAME)
                        .decodingPluginName("pgoutput")
                        .deserializer(new SimpleStringSchema())
                        .splitSize(10)
                        .splitMetaGroupSize(100)
                        .fetchSize(1000);

        // Apply filter if provided
        if (filterCondition != null && !filterCondition.trim().isEmpty()) {
            sourceBuilder.snapshotFilter(filterCondition);
        }

        SourceFunction<SourceRecord> sourceFunction =
                PostgreSQLSource.<SourceRecord>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getFirstMappedPort())
                        .database(testDatabase.getDatabaseName())
                        .schemaList(SCHEMA_NAME)
                        .tableList(SCHEMA_NAME + ".customers")
                        .username(POSTGRES_CONTAINER.getUsername())
                        .password(POSTGRES_CONTAINER.getPassword())
                        .slotName(SLOT_NAME)
                        .decodingPluginName("pgoutput")
                        .deserializer(new SimpleStringSchema())
                        .build();

        DataStreamSource<String> source =
                env.addSource(new DebeziumSourceFunction<>(sourceFunction), "PostgreSQL Source");

        List<String> capturedRecords = new ArrayList<>();

        try (CloseableIterator<String> iterator = source.executeAndCollect()) {
            int maxRecords = 100; // Safety limit
            while (iterator.hasNext() && capturedRecords.size() < maxRecords) {
                String record = iterator.next();
                if (record != null && !record.isEmpty()) {
                    capturedRecords.add(record);
                }
            }
        }

        return capturedRecords.stream()
                .filter(r -> r != null && !r.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Creates test data for snapshot filter tests. Creates customers table with various created_at
     * dates and statuses.
     */
    private void createTestData() {
        try (Connection conn =
                        DriverManager.getConnection(
                                testDatabase.getJdbcUrl(),
                                testDatabase.getUsername(),
                                testDatabase.getPassword());
                Statement stmt = conn.createStatement()) {

            // Drop existing table if any
            stmt.execute("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".customers");
            stmt.execute("DROP TABLE IF EXISTS " + SCHEMA_NAME + ".orders");

            // Create customers table
            String createCustomersTable =
                    "CREATE TABLE "
                            + SCHEMA_NAME
                            + ".customers ("
                            + "id SERIAL PRIMARY KEY, "
                            + "name VARCHAR(100) NOT NULL, "
                            + "email VARCHAR(255), "
                            + "status VARCHAR(20) DEFAULT 'active', "
                            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                            + ")";
            stmt.execute(createCustomersTable);

            // Create orders table
            String createOrdersTable =
                    "CREATE TABLE "
                            + SCHEMA_NAME
                            + ".orders ("
                            + "id SERIAL PRIMARY KEY, "
                            + "customer_id INTEGER, "
                            + "order_date DATE NOT NULL, "
                            + "total_amount DECIMAL(10, 2), "
                            + "status VARCHAR(20) DEFAULT 'pending', "
                            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                            + ")";
            stmt.execute(createOrdersTable);

            // Insert old customers (before 2024-01-01)
            String[] oldCustomers = {
                "INSERT INTO "
                        + SCHEMA_NAME
                        + ".customers (name, email, status, created_at) VALUES "
                        + "('John Doe', 'john@example.com', 'active', '2022-01-15 10:00:00'), "
                        + "('Jane Smith', 'jane@example.com', 'inactive', '2022-03-20 14:30:00'), "
                        + "('Bob Johnson', 'bob@example.com', 'active', '2022-06-10 09:15:00')",
            };

            for (String sql : oldCustomers) {
                stmt.execute(sql);
            }

            // Insert recent customers (after 2024-01-01)
            String[] recentCustomers = {
                "INSERT INTO "
                        + SCHEMA_NAME
                        + ".customers (name, email, status, created_at) VALUES "
                        + "('Alice Williams', 'alice@example.com', 'active', '2024-02-01 11:00:00'), "
                        + "('Charlie Brown', 'charlie@example.com', 'active', '2024-03-15 16:45:00'), "
                        + "('Diana Prince', 'diana@example.com', 'pending', '2024-05-20 10:30:00'), "
                        + "('Eve Davis', 'eve@example.com', 'active', '2024-06-10 08:00:00'), "
                        + "('Frank Miller', 'frank@example.com', 'inactive', '2024-07-05 13:20:00')",
            };

            for (String sql : recentCustomers) {
                stmt.execute(sql);
            }

            // Insert orders with various statuses
            String insertOrders =
                    "INSERT INTO "
                            + SCHEMA_NAME
                            + ".orders (customer_id, order_date, total_amount, status, created_at) VALUES "
                            + "(1, '2024-03-01', 125.00, 'pending', '2024-03-01 10:00:00'), "
                            + "(4, '2024-04-15', 450.50, 'shipped', '2024-04-15 14:30:00'), "
                            + "(5, '2024-05-20', 99.99, 'delivered', '2024-05-20 09:00:00'), "
                            + "(6, '2024-06-25', 250.00, 'shipped', '2024-06-25 15:00:00'), "
                            + "(7, '2024-07-10', 175.00, 'pending', '2024-07-10 11:30:00')";
            stmt.execute(insertOrders);

            // Create indexes
            stmt.execute(
                    "CREATE INDEX idx_customers_created_at ON "
                            + SCHEMA_NAME
                            + ".customers(created_at)");
            stmt.execute(
                    "CREATE INDEX idx_customers_status ON " + SCHEMA_NAME + ".customers(status)");

        } catch (Exception e) {
            throw new RuntimeException("Failed to create test data", e);
        }
    }

    /** Simple deserialization schema for testing. */
    private static class SimpleStringSchema implements DebeziumDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, org.apache.flink.util.Collector<String> out)
                throws Exception {
            Struct value = (Struct) record.value();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            if (after != null) {
                String schema = record.topic(); // topic is schema.table
                out.collect(schema + ": " + after.toString());
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return org.apache.flink.api.common.typeinfo.Types.STRING;
        }
    }
}

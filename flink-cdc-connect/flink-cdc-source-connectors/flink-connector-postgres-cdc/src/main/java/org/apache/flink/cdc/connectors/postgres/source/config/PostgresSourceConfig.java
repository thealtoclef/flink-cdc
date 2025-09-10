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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.table.catalog.ObjectPath;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;

/** The configuration for Postgres CDC source. */
public class PostgresSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final int lsnCommitCheckpointsDelay;

    public PostgresSourceConfig(
            int subtaskId,
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            @Nullable Map<ObjectPath, String> chunkKeyColumns,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst) {

        super(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumns,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);

        this.subtaskId = subtaskId;
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;

        // DEBUG: Log constructor parameters after super() call
        logConstructorParameters(
                subtaskId,
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumns,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                lsnCommitCheckpointsDelay,
                assignUnboundedChunkFirst);
    }

    private void logConstructorParameters(
            int subtaskId,
            StartupOptions startupOptions,
            List<String> databaseList,
            List<String> schemaList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            Map<ObjectPath, String> chunkKeyColumns,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst) {

        System.out.println("DEBUG: PostgresSourceConfig constructor called with parameters:");
        System.out.println("  subtaskId: " + subtaskId);
        System.out.println("  startupOptions: " + startupOptions);
        System.out.println("  databaseList: " + databaseList);
        System.out.println("  schemaList: " + schemaList);
        System.out.println("  tableList: " + tableList);
        System.out.println("  splitSize: " + splitSize);
        System.out.println("  splitMetaGroupSize: " + splitMetaGroupSize);
        System.out.println("  distributionFactorUpper: " + distributionFactorUpper);
        System.out.println("  distributionFactorLower: " + distributionFactorLower);
        System.out.println("  includeSchemaChanges: " + includeSchemaChanges);
        System.out.println("  closeIdleReaders: " + closeIdleReaders);
        System.out.println("  dbzProperties: " + dbzProperties);
        System.out.println("  dbzConfiguration: " + dbzConfiguration);
        System.out.println("  driverClassName: " + driverClassName);
        System.out.println("  hostname: " + hostname);
        System.out.println("  port: " + port);
        System.out.println("  username: " + username);
        System.out.println("  fetchSize: " + fetchSize);
        System.out.println("  serverTimeZone: " + serverTimeZone);
        System.out.println("  connectTimeout: " + connectTimeout);
        System.out.println("  connectMaxRetries: " + connectMaxRetries);
        System.out.println("  connectionPoolSize: " + connectionPoolSize);
        System.out.println("  chunkKeyColumns: " + chunkKeyColumns);
        System.out.println("  skipSnapshotBackfill: " + skipSnapshotBackfill);
        System.out.println("  isScanNewlyAddedTableEnabled: " + isScanNewlyAddedTableEnabled);
        System.out.println("  lsnCommitCheckpointsDelay: " + lsnCommitCheckpointsDelay);
        System.out.println("  assignUnboundedChunkFirst: " + assignUnboundedChunkFirst);
        System.out.println("DEBUG: PostgresSourceConfig constructor completed successfully");
    }

    /**
     * Returns {@code subtaskId} value.
     *
     * @return subtask id
     */
    public int getSubtaskId() {
        return subtaskId;
    }

    /**
     * Returns {@code lsnCommitCheckpointsDelay} value.
     *
     * @return lsn commit checkpoint delay
     */
    public int getLsnCommitCheckpointsDelay() {
        return this.lsnCommitCheckpointsDelay;
    }

    public Map<ObjectPath, String> getChunkKeyColumns() {
        return chunkKeyColumns;
    }

    /**
     * Returns the slot name for backfill task.
     *
     * @return backfill task slot name
     */
    public String getSlotNameForBackfillTask() {
        return getDbzProperties().getProperty(SLOT_NAME.name()) + "_" + getSubtaskId();
    }

    /** Returns the JDBC URL for config unique key. */
    public String getJdbcUrl() {
        return String.format(
                "jdbc:postgresql://%s:%d/%s", getHostname(), getPort(), getDatabaseList().get(0));
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return new PostgresConnectorConfig(getDbzConfiguration());
    }
}

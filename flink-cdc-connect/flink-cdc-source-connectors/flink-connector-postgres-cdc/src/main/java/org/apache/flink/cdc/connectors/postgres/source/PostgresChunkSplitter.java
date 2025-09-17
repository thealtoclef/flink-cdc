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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTypeUtils;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The splitter to split the table into chunks using primary-key (by default) or a given split key.
 */
@Internal
public class PostgresChunkSplitter extends JdbcSourceChunkSplitter {

    /** Pattern to parse table-specific filters: schema.table:condition. */
    private static final Pattern FILTER_PATTERN = Pattern.compile("([^:;]+)\\.([^:;]+):([^:;]+)");

    /** Map of table-specific filter conditions. Key: schema.table, Value: filter condition */
    private final Map<String, String> snapshotFilters;

    public PostgresChunkSplitter(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dialect,
            ChunkSplitterState chunkSplitterState) {
        super(sourceConfig, dialect, chunkSplitterState);
        this.snapshotFilters = parseSnapshotFilters(sourceConfig);
    }

    /**
     * Parse the snapshot filter configuration into a map. Format:
     * "schema.table1:condition1;schema.table2:condition2"
     */
    private Map<String, String> parseSnapshotFilters(JdbcSourceConfig sourceConfig) {
        Map<String, String> filterMap = new HashMap<>();
        if (sourceConfig instanceof PostgresSourceConfig) {
            PostgresSourceConfig postgresConfig = (PostgresSourceConfig) sourceConfig;
            String snapshotFilter = postgresConfig.getSnapshotFilter();
            if (snapshotFilter != null && !snapshotFilter.trim().isEmpty()) {
                String[] tableFilters = snapshotFilter.split(";");
                for (String tableFilter : tableFilters) {
                    Matcher matcher = FILTER_PATTERN.matcher(tableFilter.trim());
                    if (matcher.find()) {
                        String schema = matcher.group(1).trim();
                        String table = matcher.group(2).trim();
                        String condition = matcher.group(3).trim();
                        filterMap.put(schema + "." + table, condition);
                    }
                }
            }
        }
        return filterMap;
    }

    /**
     * Get the filter condition for a specific table.
     *
     * @param tableId the table identifier
     * @return the filter condition, or null if no filter is configured for this table
     */
    @Nullable
    private String getFilterForTable(TableId tableId) {
        String key = tableId.schema() + "." + tableId.table();
        return snapshotFilters.get(key);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String filterCondition = getFilterForTable(tableId);
        return PostgresQueryUtils.queryNextChunkMax(
                jdbc, tableId, splitColumn, chunkSize, includedLowerBound, filterCondition);
    }

    /** Postgres chunk split overrides queryMinMax method to query based on uuid. */
    @Override
    public Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column splitColumn)
            throws SQLException {
        String filterCondition = getFilterForTable(tableId);
        return PostgresQueryUtils.queryMinMax(jdbc, tableId, splitColumn, filterCondition);
    }

    /** Postgres chunk split overrides queryMin method to query based on uuid. */
    @Override
    public Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column splitColumn, Object excludedLowerBound)
            throws SQLException {
        return PostgresQueryUtils.queryMin(jdbc, tableId, splitColumn, excludedLowerBound);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        return PostgresQueryUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    protected DataType fromDbzColumn(Column splitColumn) {
        return PostgresTypeUtils.fromDbzColumn(splitColumn);
    }
}

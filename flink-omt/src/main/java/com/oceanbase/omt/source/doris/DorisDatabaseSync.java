/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oceanbase.omt.source.doris;

import com.oceanbase.omt.DatabaseSyncBase;
import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.partition.PartitionInfo;
import com.oceanbase.omt.utils.DataSourceUtils;
import com.oceanbase.omt.utils.OceanBaseJdbcUtils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Doris数据库同步类，实现从Doris到OceanBase的数据同步功能 */
public class DorisDatabaseSync extends DatabaseSyncBase {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDatabaseSync.class);
    private List<OceanBaseTable> oceanBaseTables;

    public DorisDatabaseSync(MigrationConfig migrationConfig) {
        super(migrationConfig);
    }

    @Override
    public Connection getConnection() throws SQLException {
        SourceMigrateConfig config = migrationConfig.getSource();
        try {
            return DataSourceUtils.getSourceDataSource(config).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void checkRequiredOptions() {
        SourceMigrateConfig source = migrationConfig.getSource();
        DorisSourceUtils.validateDorisConfig(source);
    }

    @Override
    public List<OceanBaseTable> getObTables() throws Exception {
        // 简单的缓存机制
        if (Objects.nonNull(oceanBaseTables)) {
            return oceanBaseTables;
        }

        SourceMigrateConfig source = migrationConfig.getSource();
        List<Tuple2<String, String>> tableNames = getTableNames(source.getTables());
        List<OceanBaseTable> oceanBaseTableList =
                tableNames.stream()
                        .map(ruleTable -> getTable(ruleTable.f0, ruleTable.f1))
                        .collect(Collectors.toList());
        this.oceanBaseTables = oceanBaseTableList;
        return oceanBaseTableList;
    }

    @Override
    public void createTableInOb() throws SQLException {
        List<OceanBaseTable> obTables;
        try {
            obTables = getObTables();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        OBMigrateConfig obMigrateConfig = this.migrationConfig.getOceanbase();
        String columnStoreType = obMigrateConfig.getColumnStoreType();
        if (Objects.nonNull(applyRoutesRules(obTables))) {
            obTables = applyRoutesRules(obTables).f0;
        }

        // 首先创建数据库
        List<String> databases =
                obTables.stream()
                        .map(OceanBaseTable::getDatabase)
                        .distinct()
                        .collect(Collectors.toList());
        for (String database : databases) {
            if (!OceanBaseJdbcUtils.databaseExists(migrationConfig, database)) {
                String databaseDDL = String.format("CREATE DATABASE IF NOT EXISTS %s", database);
                OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, databaseDDL);
            }
        }

        // 创建表
        List<String> obCreateTableDDLs =
                DorisDDLGenTools.buildOBCreateTableDDL(obTables, columnStoreType);

        for (String createTableDDL : obCreateTableDDLs) {
            LOG.info("Create table with DDL: {}", createTableDDL);
            OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, createTableDDL);
        }
    }

    public OceanBaseTable getTable(String databaseName, String tableName) {
        final String tableSchemaQuery =
                "SELECT `COLUMN_NAME`, `DATA_TYPE`, `ORDINAL_POSITION`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, "
                        + "`IS_NULLABLE`, `COLUMN_KEY`, `COLUMN_COMMENT`,`COLUMN_DEFAULT`,`COLUMN_TYPE` FROM `information_schema`.`COLUMNS` "
                        + "WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

        List<OceanBaseColumn> columns = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString("COLUMN_NAME");
                        String type = resultSet.getString("DATA_TYPE");
                        Integer size = resultSet.getInt("COLUMN_SIZE");
                        if (resultSet.wasNull()) {
                            size = null;
                        }

                        // Doris的布尔类型在information_schema中可能显示为tinyint
                        // 需要通过列大小来区分
                        if ("tinyint".equalsIgnoreCase(type) && size == null) {
                            type = "boolean";
                        }

                        int position = resultSet.getInt("ORDINAL_POSITION");
                        Integer scale = resultSet.getInt("DECIMAL_DIGITS");
                        if (resultSet.wasNull()) {
                            scale = null;
                        }

                        String isNullable = resultSet.getString("IS_NULLABLE");
                        String comment = resultSet.getString("COLUMN_COMMENT");
                        String defaultValue = resultSet.getString("COLUMN_DEFAULT");
                        String columnType = resultSet.getString("COLUMN_TYPE");

                        OceanBaseColumn column =
                                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                        .withName(name)
                                        .withOrdinalPosition(position - 1)
                                        .withTypeString(type)
                                        .withColumnSize(size)
                                        .withColumnType(columnType)
                                        .withDataType(type)
                                        .withNumericScale(scale)
                                        .withNullable(
                                                isNullable == null
                                                        || !isNullable.equalsIgnoreCase("NO"))
                                        .withComment(comment)
                                        .withDefaultValue(defaultValue)
                                        .build();
                        columns.add(column);

                        // 收集主键信息
                        String columnKey = resultSet.getString("COLUMN_KEY");
                        getKeys(columnKey, tableKeys, column);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to get table %s.%s", databaseName, tableName), e);
        }

        List<PartitionInfo> partitions = getPartitions(databaseName, tableName, columns);
        OceanBaseTable tableSchema = null;
        if (!columns.isEmpty()) {
            tableSchema =
                    OceanBaseTable.TableSchemaBuilder.aTableSchema()
                            .withDatabase(databaseName)
                            .withTable(tableName)
                            .withFields(columns)
                            .withPartition(partitions)
                            .build();
        }
        return tableSchema;
    }

    public SourceFunction<RowData> buildSourceFunction(OceanBaseTable oceanBaseTable) {
        SourceMigrateConfig source = migrationConfig.getSource();
        TableSchema tableSchema = DorisSourceUtils.getDorisTableSchema(source);

        // 创建Doris数据源
        // 注意：这里需要根据实际的Doris Flink Connector API来实现
        return DorisSourceUtils.createDorisSource(source, tableSchema);
    }

    public Configuration getFlinkConfiguration() {
        Configuration configuration = new Configuration();
        SourceMigrateConfig source = migrationConfig.getSource();
        Map<String, String> other = source.getOther();

        // 设置Doris相关的Flink配置
        configuration.setString("scan-url", other.get(DorisConfig.SCAN_URL));
        configuration.setString("jdbc-url", other.get(DorisConfig.JDBC_URL));
        configuration.setString("username", other.get(DorisConfig.USERNAME));
        configuration.setString("password", other.get(DorisConfig.PASSWORD));
        configuration.setString("database-name", other.get(DorisConfig.DATABASE_NAME));
        configuration.setString("table-name", other.get(DorisConfig.TABLE_NAME));

        // 设置可选配置
        String scanConnectTimeoutMs = other.get(DorisConfig.SCAN_CONNECT_TIMEOUT_MS);
        if (scanConnectTimeoutMs != null) {
            configuration.setString("scan.connect.timeout-ms", scanConnectTimeoutMs);
        }
        String scanParamsKeepAliveMin = other.get(DorisConfig.SCAN_PARAMS_KEEP_ALIVE_MIN);
        if (scanParamsKeepAliveMin != null) {
            configuration.setString("scan.params.keep-alive-min", scanParamsKeepAliveMin);
        }
        String scanParamsQueryTimeoutS = other.get(DorisConfig.SCAN_PARAMS_QUERY_TIMEOUT_S);
        if (scanParamsQueryTimeoutS != null) {
            configuration.setString("scan.params.query-timeout-s", scanParamsQueryTimeoutS);
        }
        String scanParamsMemLimitByte = other.get(DorisConfig.SCAN_PARAMS_MEM_LIMIT_BYTE);
        if (scanParamsMemLimitByte != null) {
            configuration.setString("scan.params.mem-limit-byte", scanParamsMemLimitByte);
        }
        String scanMaxRetries = other.get(DorisConfig.SCAN_MAX_RETRIES);
        if (scanMaxRetries != null) {
            configuration.setString("scan.max-retries", scanMaxRetries);
        }

        return configuration;
    }

    private List<PartitionInfo> getPartitions(
            String databaseName, String tableName, List<OceanBaseColumn> columns) {
        try (Connection connection = getConnection()) {
            return DorisJdbcUtils.obtainPartitionInfo(connection, databaseName, tableName, columns);
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to get partition info for table {}.{}: {}",
                    databaseName,
                    tableName,
                    e.getMessage());
            return new ArrayList<>();
        }
    }

    private void getKeys(String columnKey, List<String> tableKeys, OceanBaseColumn column) {
        if (!StringUtils.isNullOrWhitespaceOnly(columnKey) && columnKey.equalsIgnoreCase("PRI")) {
            tableKeys.add(column.getName());
        }
    }

    public List<Tuple2<String, String>> getTableNames(String tables) {
        String sql =
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.`TABLES` WHERE CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) REGEXP ?";
        try {
            return DorisJdbcUtils.executeDoubleColumnStatement(getConnection(), sql, tables);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table names", e);
        }
    }
}

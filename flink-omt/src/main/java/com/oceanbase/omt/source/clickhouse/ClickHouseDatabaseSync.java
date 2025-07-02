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
package com.oceanbase.omt.source.clickhouse;

import com.oceanbase.omt.DatabaseSyncBase;
import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.partition.PartitionInfo;
import com.oceanbase.omt.source.clickhouse.enums.ColumnInfo;
import com.oceanbase.omt.utils.DataSourceUtils;
import com.oceanbase.omt.utils.OceanBaseJdbcUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/** @author yixing */
public class ClickHouseDatabaseSync extends DatabaseSyncBase {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseDatabaseSync.class);
    private List<OceanBaseTable> oceanBaseTables;

    public ClickHouseDatabaseSync(MigrationConfig migrationConfig) {
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
    public void checkRequiredOptions() {}

    @Override
    public List<OceanBaseTable> getObTables() {
        // Only as a simple cache.
        if (Objects.nonNull(oceanBaseTables)) {
            return oceanBaseTables;
        }
        SourceMigrateConfig source = migrationConfig.getSource();
        List<Tuple2<String, String>> tableNames =
                getTableNames(source.getDatabase(), source.getTables());
        List<OceanBaseTable> oceanBaseTableList =
                tableNames.stream()
                        .map(ruleTable -> getTable(ruleTable.f0, ruleTable.f1))
                        .collect(Collectors.toList());
        this.oceanBaseTables = oceanBaseTableList;
        return oceanBaseTableList;
    }

    public OceanBaseTable getTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        final String tableSchemaQuery =
                "SELECT database,table,name,type,default_kind,default_expression,comment,is_in_partition_key,is_in_sorting_key,is_in_primary_key,is_in_sampling_key,numeric_precision,numeric_scale,comment,is_in_partition_key,is_in_sorting_key,is_in_primary_key,is_in_sampling_key,numeric_precision,numeric_scale\"\n"
                        + "                                      + \" FROM system.columns\n"
                        + "WHERE database = ? AND table = ?";

        List<OceanBaseColumn> columns = new ArrayList<>();
        List<String> partitionTypes = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString(ColumnInfo.NAME.getName());
                        String type = resultSet.getString(ColumnInfo.TYPE.getName());
                        if (type.contains(ClickHouseType.Decimal)) {
                            type = ClickHouseType.Decimal;
                        }
                        if (type.contains(ClickHouseType.String)) {
                            type = ClickHouseType.String;
                        }
                        boolean isPartitionKey =
                                resultSet.getBoolean(ColumnInfo.IS_IN_PARTITION_KEY.getName());
                        boolean isPrimaryKey =
                                resultSet.getBoolean(ColumnInfo.IS_IN_PRIMARY_KEY.getName());
                        boolean isSortingKey =
                                resultSet.getBoolean(ColumnInfo.IS_IN_SORTING_KEY.getName());
                        if (isPartitionKey) {
                            tableKeys.add(name);
                            partitionTypes.add(type);
                            primaryKeys.add(name);
                        }
                        if (isPrimaryKey) {
                            primaryKeys.add(name.trim());
                        }
                        if (isSortingKey) {
                            primaryKeys.add(name.trim());
                        }
                        String comment = resultSet.getString(ColumnInfo.COMMENT.getName());
                        int numericPrecision =
                                resultSet.getInt(ColumnInfo.NUMERIC_PRECISION.getName());
                        int numericScale = resultSet.getInt(ColumnInfo.NUMERIC_SCALE.getName());

                        OceanBaseColumn column =
                                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                        .withName(name)
                                        .withDataType(type)
                                        .withTypeString(type)
                                        .withComment(comment)
                                        .withColumnSize(numericPrecision)
                                        .withNumericScale(numericScale)
                                        .build();

                        columns.add(column);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        List<PartitionInfo> partitions = getPartitions(databaseName, tableName, columns);
        Map<String, String> keyMap = new HashMap<>();
        getKeys(databaseName, tableName, keyMap);
        for (PartitionInfo partition : partitions) {
            partition.withPartitionKey(String.join(",", tableKeys));
            partition.withPartitionKeyType(partitionTypes);
            partition.withPartitionKeyExpression(keyMap.get("partition_key"));
        }

        return OceanBaseTable.TableSchemaBuilder.aTableSchema()
                .withDatabase(databaseName)
                .withTable(tableName)
                .withFields(columns)
                .withKeys(primaryKeys)
                .withPartition(partitions)
                .build();
    }

    private void getKeys(String databaseName, String tableName, Map<String, String> kyeMap) {
        String sql =
                "SELECT partition_key,sorting_key,primary_key\n"
                        + "FROM system.tables\n"
                        + "WHERE database = ? AND name = ?";
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        kyeMap.put("primary_key", resultSet.getString("primary_key"));
                        kyeMap.put("sorting_key", resultSet.getString("sorting_key"));
                        kyeMap.put("partition_key", resultSet.getString("partition_key"));
                    }
                }
            } catch (SQLException e) {
                LOG.error("getKeys error", e);
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            LOG.error("getKeys error", e);
            throw new RuntimeException(e);
        }
    }

    public List<PartitionInfo> getPartitions(
            String databaseName, String tableName, List<OceanBaseColumn> columns) {
        try {
            return ClickHouseJdbcUtils.obtainPartitionInfo(
                    getConnection(), databaseName, tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table names", e);
        }
    }

    public List<Tuple2<String, String>> getTableNames(String databases, String tables) {
        String sql =
                "SELECT database, name FROM system.tables WHERE match(database,?) and match(name,?)";
        try {
            return ClickHouseJdbcUtils.executeDoubleColumnStatement(
                    getConnection(), sql, databases, tables);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTableInOb() {
        List<OceanBaseTable> obTables = null;
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
        List<String> databases =
                obTables.stream()
                        .map(OceanBaseTable::getDatabase)
                        .distinct()
                        .collect(Collectors.toList());
        for (String database : databases) {
            if (!OceanBaseJdbcUtils.databaseExists(migrationConfig, database)) {
                String databaseDDL = String.format("CREATE DATABASE IF NOT EXISTS %s", database);
                try {
                    OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, databaseDDL);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        List<String> obCreateTableDDLs =
                ClickHouseDDLGenTools.buildOBCreateTableDDL(obTables, columnStoreType);

        for (String createTableDDL : obCreateTableDDLs) {
            LOG.info("Create table with DDL: {}", createTableDDL);
            try {
                OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, createTableDDL);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SourceFunction<RowData> buildSourceFunction(OceanBaseTable oceanBaseTable) {
        Map<String, String> other = migrationConfig.getSource().getOther();
        String jdbcUrl = other.get(ClickHouseConfig.JDBC_URL).replace("jdbc:", "");

        ClickHouseReadOptions clickHouseReadOptions =
                new ClickHouseReadOptions.Builder()
                        .withUrl(jdbcUrl)
                        .withDatabaseName(oceanBaseTable.getDatabase())
                        .withTableName(oceanBaseTable.getTable())
                        .build();
        List<OceanBaseColumn> fields = oceanBaseTable.getFields();
        LogicalType[] fieldTypes =
                fields.stream().map(ClickHouseType::toFlinkType).toArray(LogicalType[]::new);

        DataType[] dataTypes =
                Arrays.stream(fieldTypes).map(DataTypes::of).toArray(DataType[]::new);

        List<String> fieldNames =
                fields.stream().map(OceanBaseColumn::getName).collect(Collectors.toList());

        RowType rowType = RowType.of(fieldTypes, fieldNames.toArray(new String[0]));
        TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);

        Properties connProps = new Properties();
        connProps.setProperty(ClickHouseConfig.USERNAME, other.get(ClickHouseConfig.USERNAME));
        connProps.setProperty(ClickHouseConfig.PASSWORD, other.get(ClickHouseConfig.PASSWORD));

        return ClickHouseSourceUtils.createClickHouseSource(
                clickHouseReadOptions,
                connProps,
                fieldNames.toArray(new String[0]),
                dataTypes,
                rowTypeInfo,
                fieldTypes);
    }
}

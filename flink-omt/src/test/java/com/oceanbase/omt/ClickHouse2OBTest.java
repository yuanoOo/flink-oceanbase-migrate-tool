package com.oceanbase.omt;


import static com.oceanbase.omt.source.starrocks.StarRocksJdbcUtils.executeDoubleColumnStatement;
import static com.oceanbase.omt.source.starrocks.StarRocksJdbcUtils.obtainPartitionInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.log.Log;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.omt.base.OceanBaseMySQLTestBase;
import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.catalog.OceanBaseTable.TableSchemaBuilder;
import com.oceanbase.omt.catalog.TableIdentifier;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.partition.PartitionInfo;
import com.oceanbase.omt.partition.PartitionUtils;
import com.oceanbase.omt.partition.PartitionUtils.RangeInfo;
import com.oceanbase.omt.source.clickhouse.ClickHouseDDLGenTools;
import com.oceanbase.omt.source.clickhouse.ClickHouseDatabaseSync;
import com.oceanbase.omt.source.clickhouse.ClickHouseJdbcUtils;
import com.oceanbase.omt.source.clickhouse.ClickHouseType;
import com.oceanbase.omt.source.clickhouse.enums.ColumnInfo;
import com.oceanbase.omt.source.starrocks.StarRocksDDLGenTools;
import com.oceanbase.omt.source.starrocks.StarRocksType;
import com.oceanbase.omt.utils.DataSourceUtils;
import com.oceanbase.omt.utils.OBSinkType;
import com.oceanbase.omt.utils.OceanBaseJdbcUtils;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;

public class ClickHouse2OBTest extends OceanBaseMySQLTestBase {
    protected MigrationConfig migrationConfig;
    private static final Logger LOG = LoggerFactory.getLogger(StarRocks2OBTest.class);


    @Before
    public void init() throws IOException {
        migrationConfig = YamlParser.parseResource("clickhouse.yaml");
    }

    @Test
    public void setUp() throws Exception {
        migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        crateDataBases(sourceDataSource.getConnection(), "test1", "test2");
        initialize(sourceDataSource.getConnection(), "sql/clickHouse-sql.sql");
    }



    @Test
    public void close() throws IOException, SQLException {
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        // drop source
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        dropDataBases(sourceDataSource.getConnection(), "test1", "test2");
    }
    @Test
    public void getSourceConnection() throws Exception {
        SourceMigrateConfig source = migrationConfig.getSource();
        LOG.info("source:{}", source);
        //DataSource sourceDataSource =
        //        DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        //crateDataBases(sourceDataSource.getConnection(), "test1", "test2");

    }

    public List<OceanBaseTable> getObTables() throws IOException {
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        SourceMigrateConfig source = migrationConfig.getSource();
        String tables = source.getTables();
        String database = source.getDatabase();
        String sql = "SELECT database, name FROM system.tables WHERE match(database,?) and match(name,?)";
        List<Tuple2<String, String>> tableNames =
                ClickHouseJdbcUtils.executeDoubleColumnStatement(getConnection(), sql,database,tables);
        List<OceanBaseTable> oceanBaseTableList =
                tableNames.stream()
                        .map(ruleTable -> getTable(ruleTable.f0, ruleTable.f1))
                        .collect(Collectors.toList());
        return oceanBaseTableList;
    }
    @Test
    public void createTable() throws IOException, SQLException {
        OBMigrateConfig obMigrateConfig = this.migrationConfig.getOceanbase();
        getObTables();
        String columnStoreType = obMigrateConfig.getColumnStoreType();
        List<OceanBaseTable> obTables = getObTables();
        List<String> databases =
                obTables.stream()
                        .map(OceanBaseTable::getDatabase)
                        .distinct()
                        .collect(Collectors.toList());
        LOG.info("obMigrateConfig:{}",obMigrateConfig);
        // create database
        for (String database : databases) {
            if (!OceanBaseJdbcUtils.databaseExists(migrationConfig, database)) {
                String databaseDDL = String.format("CREATE DATABASE IF NOT EXISTS %s", database);
                OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, databaseDDL);
            }
        }
        List<String> obCreateTableDDLs =
                ClickHouseDDLGenTools.buildOBCreateTableDDL(obTables, columnStoreType);

    }
    @Test
    public void test() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        ClickHouseDatabaseSync clickHouseDatabaseSync = new ClickHouseDatabaseSync(migrationConfig);
        List<OceanBaseTable> obTables = getObTables();
        List<String> obCreateTableDDLs = buildOBCreateTableDDL(obTables, "");
        LOG.info("strings:{}",obCreateTableDDLs);
        for (String createTableDDL : obCreateTableDDLs) {
            LOG.info("Create table with DDL: {}", createTableDDL);
            OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, createTableDDL);
        }
        clickHouseDatabaseSync.buildPipeline(env);
    }
    public List<String> buildOBCreateTableDDL(List<OceanBaseTable> schemaList, String columnStoreType){
        String prefix = "CREATE TABLE IF NOT EXISTS %s (%s %s)";
        String columnStoreSql = "";
        if (StringUtils.isNotBlank(columnStoreType)) {
            if (columnStoreType.equalsIgnoreCase("CST")) {
                columnStoreSql = " WITH COLUMN GROUP(each column)";
            }
            if (columnStoreType.equalsIgnoreCase("RCMT")) {
                columnStoreSql = " WITH COLUMN GROUP(all columns, each column)";
            }
        }
        // name type default-value comment
        String colFormat = "`%s` %s %s %s";
        String finalColumnStoreSql = columnStoreSql;
        List<String> tableDDLs =
                schemaList.stream()
                        .map(
                                schema -> {
                                    // Relate column
                                    List<OceanBaseColumn> fields = schema.getFields();
                                    String filedStr =
                                            fields.stream()
                                                    .map(
                                                            fieldSchema -> {
                                                                String oceanBaseType =
                                                                        ClickHouseType
                                                                                .toOceanBaseMySQLType(
                                                                                        fieldSchema);
                                                                return String.format(
                                                                        colFormat,
                                                                        fieldSchema.getName(),
                                                                        oceanBaseType,
                                                                        buildDefaultValue(
                                                                                fieldSchema),
                                                                        buildComment(fieldSchema));
                                                            })
                                                    .collect(Collectors.joining(","));

                                    // Relate table
                                    String tableName =
                                            String.format(
                                                    "`%s`.`%s`",
                                                    schema.getDatabase(), schema.getTable());
                                    List<String> keys = schema.getKeys();
                                    String primaryKeyStr = "";
                                    if (!CollectionUtil.isNullOrEmpty(keys)) {
                                        primaryKeyStr =
                                                String.format(
                                                        ",PRIMARY KEY(%s)", String.join(",", keys));
                                    }

                                    String noPartitionDDl =
                                            String.format(
                                                    prefix, tableName, filedStr, primaryKeyStr);
                                    String obPartitionWithDDL =
                                            buildOBPartitionWithDDL(
                                                    noPartitionDDl, schema.getPartition());
                                    return obPartitionWithDDL + finalColumnStoreSql;
                                })
                        .collect(Collectors.toList());
        return tableDDLs;
    }


    public Connection getConnection(){
        SourceMigrateConfig config = migrationConfig.getSource();
        try {
            return DataSourceUtils.getSourceDataSource(config).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testJdbc() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain StarRocks MetaData
        ClickHouseDatabaseSync clickHouseDatabaseSync = new ClickHouseDatabaseSync(migrationConfig);
        clickHouseDatabaseSync.createTableInOb();
        clickHouseDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

    }

    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        assertEqualsInAnyOrder(expected, actual);
    }


    public OceanBaseTable getTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        final String tableSchemaQuery="SELECT database,table,name,type,default_kind,default_expression,comment,is_in_partition_key,is_in_sorting_key,is_in_primary_key,is_in_sampling_key,numeric_precision,numeric_scale,comment,is_in_partition_key,is_in_sorting_key,is_in_primary_key,is_in_sampling_key,numeric_precision,numeric_scale"
                                      + " FROM system.columns\n"
                                      + "WHERE database = ? AND table = ?";

        List<OceanBaseColumn> columns = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString(ColumnInfo.NAME.getName());
                        String type = resultSet.getString(ColumnInfo.TYPE.getName());
                        if (type.contains(ClickHouseType.DECIMAL)){
                            type=ClickHouseType.DECIMAL;
                        }
                        if (type.contains(ClickHouseType.LOWCARDINALITY)){
                            type=ClickHouseType.LOWCARDINALITY;
                        }
                        String comment = resultSet.getString(ColumnInfo.COMMENT.getName());
                        int numericPrecision = resultSet.getInt(ColumnInfo.NUMERIC_PRECISION.getName());
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
        Map<String, String> keyMap=new HashMap<>();
        getKeys(databaseName, tableName,keyMap);
        String primaryKey = keyMap.get("primary_key");
        tableKeys.add(primaryKey);
        for (PartitionInfo partition : partitions) {
            partition.withPartitionKey(keyMap.get("partition_key"));
        }
        OceanBaseTable tableSchema  =
                OceanBaseTable.TableSchemaBuilder.aTableSchema()
                        .withDatabase(databaseName)
                        .withTable(tableName)
                        .withFields(columns)
                        .withKeys(tableKeys)
                        .withPartition(partitions)
                        .build();
        return tableSchema;
    }


    @Test
    public void getTable(){
        String databaseName = "test1";
        String tableName = "orders3";
        final String tableSchemaQuery="SELECT database,table,name,type,default_kind,default_expression,comment,is_in_partition_key,is_in_sorting_key,is_in_primary_key,is_in_sampling_key\n"
                                      + "FROM system.columns\n"
                                      + "WHERE database = ? AND table = ?";

        List<OceanBaseColumn> columns = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString(ColumnInfo.NAME.getName());
                        String type = resultSet.getString(ColumnInfo.TYPE.getName());
                        String comment = resultSet.getString(ColumnInfo.COMMENT.getName());
                        OceanBaseColumn column =
                                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                        .withName(name)
                                        .withDataType(type)
                                        .withTypeString(type)
                                        .withComment(comment)
                                        .build();

                        columns.add(column);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        List<PartitionInfo> partitions = getPartitions(databaseName, tableName, columns);
        OceanBaseTable tableSchema  =
                OceanBaseTable.TableSchemaBuilder.aTableSchema()
                        .withDatabase(databaseName)
                        .withTable(tableName)
                        .withFields(columns)
                        .withKeys(tableKeys)
                        .withPartition(partitions)
                        .build();

        LOG.info("columns:{}", columns);
        LOG.info("tableSchema:{}", tableSchema);
    }
    public List<PartitionInfo> getPartitions(
            String databaseName, String tableName, List<OceanBaseColumn> columns) {
        return obtainPartitionInfo(databaseName, tableName);
    }

    public List<PartitionInfo> obtainPartitionInfo(String databaseName, String tableName){
        String sql = "SELECT *\n"
                     + "FROM system.parts\n"
                     + "WHERE database = ? AND table = ?";
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()){
                    String partitionName = resultSet.getString("partition");
                    if (!partitionName.equals("tuple()")){
                        PartitionInfo partitionInfo = new PartitionInfo();
                        partitionInfo.withPartitionName(partitionName);
                        partitionInfos.add(partitionInfo);
                    }
                }
            }
            return partitionInfos;
        }catch (SQLException e) {
            LOG.error("getPartitions error", e);
            throw new RuntimeException(e);
        }

    }

    private static String buildDefaultValue(OceanBaseColumn fieldSchema) {
        String defaultValue = "";
        if (!Strings.isNullOrEmpty(fieldSchema.getDefaultValue())) {
            defaultValue = String.format("DEFAULT %s", fieldSchema.getDefaultValue());
        }
        return defaultValue;
    }

    private static String buildComment(OceanBaseColumn fieldSchema) {
        String comment = "";
        if (!Strings.isNullOrEmpty(fieldSchema.getComment())) {
            comment = String.format("COMMENT '%s'", fieldSchema.getComment());
        }
        return comment;
    }

    public static String buildOBPartitionWithDDL(String ddl, List<PartitionInfo> partitions) {
        String partitionTemplate="PARTITION BY LIST (%s) (";
        String partitionListTemplate = "PARTITION %s VALUES IN (%s)";
        if (partitions.isEmpty()){
            return ddl;
        }
        String partitionKey = partitions.get(0).getPartitionKey();
        if (partitionKey.contains("toYYYYMM")){
            String expression="YEAR(%s) * 100 + MONTH(%s)";
            int start = partitionKey.indexOf("(") + 1;
            int end = partitionKey.indexOf(")");
            String content = partitionKey.substring(start, end);
            String expressionFormat=String.format(expression, content, content);
            ddl+= String.format(partitionTemplate, expressionFormat);
        }
        for (int i = 0; i < partitions.size(); i++) {
            String partitionName = partitions.get(i).getPartitionName();
            if (i==partitions.size()-1){
                ddl += String.format(partitionListTemplate, "p_" + partitionName, partitionName);
            }else {
                ddl += String.format(partitionListTemplate, "p_" + partitionName, partitionName) + ",";
            }
        }
        ddl+=");";
        return ddl;
    }

    private void getKeys(String databaseName, String tableName, Map<String,String> kyeMap){
       String sql="SELECT partition_key,sorting_key,primary_key\n"
                  + "FROM system.tables\n"
                  + "WHERE database = ? AND name = ?";
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        kyeMap.put("primary_key",resultSet.getString("primary_key"));
                        kyeMap.put("sorting_key",resultSet.getString("sorting_key"));
                        kyeMap.put("partition_key",resultSet.getString("partition_key"));
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
    @Test
    public void testGetKeys(){
        Map<String,String> tableKeys=new HashMap<>();
        getKeys("test1","orders1",tableKeys);
        LOG.info("tableKeys:{}", tableKeys);
    }


}

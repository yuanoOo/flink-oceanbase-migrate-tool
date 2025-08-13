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
package com.oceanbase.omt;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.source.doris.DorisDatabaseSync;
import com.oceanbase.omt.source.doris.DorisTypeConverter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Doris到OceanBase的集成测试类 */
public class Doris2OBTest {

    @Test
    public void testDorisTypeConverter() {
        DorisTypeConverter converter = new DorisTypeConverter();

        // 测试基本数据类型转换
        assertEquals("TINYINT", DorisTypeConverter.convertToOceanBaseType("TINYINT"));
        assertEquals("SMALLINT", DorisTypeConverter.convertToOceanBaseType("SMALLINT"));
        assertEquals("INT", DorisTypeConverter.convertToOceanBaseType("INT"));
        assertEquals("BIGINT", DorisTypeConverter.convertToOceanBaseType("BIGINT"));
        assertEquals("BIGINT", DorisTypeConverter.convertToOceanBaseType("LARGEINT"));
        assertEquals("FLOAT", DorisTypeConverter.convertToOceanBaseType("FLOAT"));
        assertEquals("DOUBLE", DorisTypeConverter.convertToOceanBaseType("DOUBLE"));
        assertEquals("DECIMAL(10,2)", DorisTypeConverter.convertToOceanBaseType("DECIMAL(10,2)"));
        assertEquals("BOOLEAN", DorisTypeConverter.convertToOceanBaseType("BOOLEAN"));

        // 测试字符串类型转换
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("STRING"));
        assertEquals("VARCHAR(100)", DorisTypeConverter.convertToOceanBaseType("VARCHAR(100)"));
        assertEquals("CHAR(10)", DorisTypeConverter.convertToOceanBaseType("CHAR(10)"));
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("TEXT"));

        // 测试日期时间类型转换
        assertEquals("DATE", DorisTypeConverter.convertToOceanBaseType("DATE"));
        assertEquals("DATETIME", DorisTypeConverter.convertToOceanBaseType("DATETIME"));
        assertEquals("DATETIME", DorisTypeConverter.convertToOceanBaseType("TIMESTAMP"));

        // 测试JSON类型转换
        assertEquals("JSON", DorisTypeConverter.convertToOceanBaseType("JSON"));

        // 测试复杂类型转换
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("ARRAY<INT>"));
        assertEquals(
                "VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("MAP<STRING,INT>"));
        assertEquals(
                "VARCHAR(65535)",
                DorisTypeConverter.convertToOceanBaseType("STRUCT<name:STRING,age:INT>"));

        // 测试Doris特有类型转换
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("HLL"));
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("BITMAP"));
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("QUANTILE_STATE"));

        // 测试默认情况
        assertEquals("VARCHAR(65535)", DorisTypeConverter.convertToOceanBaseType("UNKNOWN_TYPE"));
    }

    @Test
    public void testDorisDatabaseSyncCreation() {
        // 创建测试配置
        MigrationConfig migrationConfig = createTestMigrationConfig();

        // 创建DorisDatabaseSync实例
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(migrationConfig);

        // 验证实例创建成功
        assertNotNull(dorisDatabaseSync);
    }

    @Test
    public void testDorisConfigValidation() {
        // 创建有效的配置
        MigrationConfig validConfig = createTestMigrationConfig();
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(validConfig);

        // 验证配置检查不会抛出异常
        dorisDatabaseSync.checkRequiredOptions();
    }

    @Test
    public void testDorisTableSchema() {
        // 创建测试表结构
        OceanBaseTable testTable = createTestTable();

        // 验证表结构
        assertNotNull(testTable);
        assertEquals("test_db", testTable.getDatabase());
        assertEquals("test_table", testTable.getTable());
        assertEquals(3, testTable.getFields().size());

        // 验证字段
        OceanBaseColumn idColumn = testTable.getFields().get(0);
        assertEquals("id", idColumn.getName());
        assertEquals("INT", idColumn.getTypeString());
        // 注意：OceanBaseColumn类没有isPrimaryKey方法，主键信息存储在表的keys中
        assertTrue(testTable.getKeys().contains("id"));

        OceanBaseColumn nameColumn = testTable.getFields().get(1);
        assertEquals("name", nameColumn.getName());
        assertEquals("VARCHAR", nameColumn.getTypeString());

        OceanBaseColumn createTimeColumn = testTable.getFields().get(2);
        assertEquals("create_time", createTimeColumn.getName());
        assertEquals("DATETIME", createTimeColumn.getTypeString());
    }

    /** 创建测试用的MigrationConfig */
    private MigrationConfig createTestMigrationConfig() {
        MigrationConfig config = new MigrationConfig();

        // 创建源配置
        SourceMigrateConfig sourceConfig = new SourceMigrateConfig();
        sourceConfig.setType("doris");
        sourceConfig.setDatabase("test_db");
        sourceConfig.setTables("test_table");

        // 添加Doris特定配置
        sourceConfig.setOther("scan-url", "localhost:8030");
        sourceConfig.setOther("jdbc-url", "jdbc:mysql://localhost:9030/test_db");
        sourceConfig.setOther("username", "root");
        sourceConfig.setOther("password", "password");
        sourceConfig.setOther("database-name", "test_db");
        sourceConfig.setOther("table-name", "test_table");

        config.setSource(sourceConfig);

        // 创建OceanBase配置
        OBMigrateConfig obConfig = new OBMigrateConfig();
        obConfig.setUrl("jdbc:mysql://localhost:2881/test");
        obConfig.setUsername("root@test");
        obConfig.setPassword("password");
        obConfig.setSchemaName("test");
        obConfig.setType("jdbc");

        config.setOceanbase(obConfig);

        return config;
    }

    /** 创建测试用的OceanBaseTable */
    private OceanBaseTable createTestTable() {
        OceanBaseColumn idColumn =
                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                        .withName("id")
                        .withOrdinalPosition(0)
                        .withTypeString("INT")
                        .withDataType("INT")
                        .withNullable(false)
                        .build();

        OceanBaseColumn nameColumn =
                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                        .withName("name")
                        .withOrdinalPosition(1)
                        .withTypeString("VARCHAR")
                        .withDataType("VARCHAR")
                        .withNullable(true)
                        .build();

        OceanBaseColumn createTimeColumn =
                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                        .withName("create_time")
                        .withOrdinalPosition(2)
                        .withTypeString("DATETIME")
                        .withDataType("DATETIME")
                        .withNullable(true)
                        .build();

        return OceanBaseTable.TableSchemaBuilder.aTableSchema()
                .withDatabase("test_db")
                .withTable("test_table")
                .withFields(java.util.Arrays.asList(idColumn, nameColumn, createTimeColumn))
                .withKeys(java.util.Arrays.asList("id"))
                .build();
    }
}

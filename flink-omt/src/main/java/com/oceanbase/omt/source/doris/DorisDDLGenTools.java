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

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Doris DDL生成工具类，用于生成OceanBase的建表DDL语句 */
public class DorisDDLGenTools {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDDLGenTools.class);

    /**
     * 为OceanBase表列表生成建表DDL语句
     *
     * @param oceanBaseTables OceanBase表列表
     * @param columnStoreType 列存储类型
     * @return DDL语句列表
     */
    public static List<String> buildOBCreateTableDDL(
            List<OceanBaseTable> oceanBaseTables, String columnStoreType) {
        List<String> ddlList = new ArrayList<>();

        for (OceanBaseTable table : oceanBaseTables) {
            String ddl = buildCreateTableDDL(table, columnStoreType);
            ddlList.add(ddl);
        }

        return ddlList;
    }

    /**
     * 为单个OceanBase表生成建表DDL语句
     *
     * @param table OceanBase表
     * @param columnStoreType 列存储类型
     * @return DDL语句
     */
    public static String buildCreateTableDDL(OceanBaseTable table, String columnStoreType) {
        StringBuilder ddl = new StringBuilder();

        // 添加CREATE TABLE语句
        ddl.append("CREATE TABLE IF NOT EXISTS `")
                .append(table.getDatabase())
                .append("`.`")
                .append(table.getTable())
                .append("` (");

        // 添加列定义
        List<String> columnDefinitions = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();

        for (OceanBaseColumn column : table.getFields()) {
            String columnDef = buildColumnDefinition(column);
            columnDefinitions.add(columnDef);
        }

        // 从表的keys中获取主键信息
        if (table.getKeys() != null) {
            for (String key : table.getKeys()) {
                primaryKeys.add("`" + key + "`");
            }
        }

        ddl.append(String.join(", ", columnDefinitions));

        // 添加主键约束
        if (!primaryKeys.isEmpty()) {
            ddl.append(", PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
        }

        ddl.append(")");

        // 添加表属性
        if (columnStoreType != null && !columnStoreType.isEmpty()) {
            ddl.append(" ENGINE = ").append(columnStoreType);
        }

        // 添加表注释
        if (table.getTableComment() != null && !table.getTableComment().isEmpty()) {
            ddl.append(" COMMENT = '").append(table.getTableComment()).append("'");
        }

        ddl.append(";");

        LOG.debug(
                "Generated DDL for table {}.{}: {}",
                table.getDatabase(),
                table.getTable(),
                ddl.toString());

        return ddl.toString();
    }

    /**
     * 构建列定义
     *
     * @param column 列信息
     * @return 列定义字符串
     */
    private static String buildColumnDefinition(OceanBaseColumn column) {
        StringBuilder columnDef = new StringBuilder();

        // 列名
        columnDef.append("`").append(column.getName()).append("` ");

        // 数据类型
        String dataType = column.getTypeString();
        if (dataType != null) {
            // 使用Doris类型转换器转换数据类型
            String convertedType = DorisTypeConverter.convertToOceanBaseType(dataType);
            columnDef.append(convertedType);
        }

        // 是否允许NULL
        if (column.getNullable() != null && !column.getNullable()) {
            columnDef.append(" NOT NULL");
        }

        // 默认值
        if (column.getDefaultValue() != null && !column.getDefaultValue().isEmpty()) {
            columnDef.append(" DEFAULT ").append(column.getDefaultValue());
        }

        // 列注释
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            columnDef.append(" COMMENT '").append(column.getComment()).append("'");
        }

        return columnDef.toString();
    }

    /**
     * 生成删除表的DDL语句
     *
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return DDL语句
     */
    public static String buildDropTableDDL(String databaseName, String tableName) {
        return String.format("DROP TABLE IF EXISTS `%s`.`%s`;", databaseName, tableName);
    }

    /**
     * 生成创建数据库的DDL语句
     *
     * @param databaseName 数据库名
     * @return DDL语句
     */
    public static String buildCreateDatabaseDDL(String databaseName) {
        return String.format("CREATE DATABASE IF NOT EXISTS `%s`;", databaseName);
    }
}

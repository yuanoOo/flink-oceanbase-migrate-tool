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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/** Doris数据类型定义类 */
public class DorisType {

    // 基本数值类型
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String LARGEINT = "LARGEINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";

    // 布尔类型
    public static final String BOOLEAN = "BOOLEAN";

    // 字符串类型
    public static final String STRING = "STRING";
    public static final String VARCHAR = "VARCHAR";
    public static final String CHAR = "CHAR";
    public static final String TEXT = "TEXT";

    // 日期时间类型
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";

    // JSON类型
    public static final String JSON = "JSON";

    // 复杂类型
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";

    // Doris特有类型
    public static final String HLL = "HLL";
    public static final String BITMAP = "BITMAP";
    public static final String QUANTILE_STATE = "QUANTILE_STATE";

    /** 将Doris数据类型转换为Flink数据类型 */
    public static DataType toFlinkType(String dorisType) {
        if (dorisType == null) {
            return DataTypes.STRING();
        }

        String upperType = dorisType.toUpperCase();

        if (upperType.startsWith(TINYINT)) return DataTypes.TINYINT();
        if (upperType.startsWith(SMALLINT)) return DataTypes.SMALLINT();
        if (upperType.startsWith(INT)) return DataTypes.INT();
        if (upperType.startsWith(BIGINT) || upperType.startsWith(LARGEINT))
            return DataTypes.BIGINT();
        if (upperType.startsWith(FLOAT)) return DataTypes.FLOAT();
        if (upperType.startsWith(DOUBLE)) return DataTypes.DOUBLE();
        if (upperType.startsWith(DECIMAL)) return DataTypes.DECIMAL(38, 18);
        if (upperType.startsWith(BOOLEAN)) return DataTypes.BOOLEAN();
        if (upperType.startsWith(STRING)
                || upperType.startsWith(VARCHAR)
                || upperType.startsWith(CHAR)
                || upperType.startsWith(TEXT)) return DataTypes.STRING();
        if (upperType.startsWith(DATE)) return DataTypes.DATE();
        if (upperType.startsWith(DATETIME) || upperType.startsWith(TIMESTAMP))
            return DataTypes.TIMESTAMP();
        if (upperType.startsWith(JSON)) return DataTypes.STRING();
        if (upperType.startsWith(ARRAY)
                || upperType.startsWith(MAP)
                || upperType.startsWith(STRUCT)
                || upperType.startsWith(HLL)
                || upperType.startsWith(BITMAP)
                || upperType.startsWith(QUANTILE_STATE)) return DataTypes.STRING();

        return DataTypes.STRING();
    }

    /** 将OceanBaseColumn转换为Flink LogicalType */
    public static LogicalType toFlinkDataType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString().toUpperCase()) {
                // Numeric
            case BOOLEAN:
                return DataTypes.BOOLEAN().getLogicalType();
            case TINYINT:
                return DataTypes.TINYINT().getLogicalType();
            case SMALLINT:
                return DataTypes.SMALLINT().getLogicalType();
            case INT:
                return DataTypes.INT().getLogicalType();
            case BIGINT:
            case LARGEINT:
                return DataTypes.BIGINT().getLogicalType();
            case FLOAT:
                return DataTypes.FLOAT().getLogicalType();
            case DOUBLE:
                return DataTypes.DOUBLE().getLogicalType();
            case DECIMAL:
                return DataTypes.DECIMAL(fieldSchema.getColumnSize(), fieldSchema.getNumericScale())
                        .getLogicalType();
                // String
            case CHAR:
            case VARCHAR:
            case STRING:
            case TEXT:
                return DataTypes.STRING().getLogicalType();
                // Date
            case DATE:
                return DataTypes.DATE().getLogicalType();
            case DATETIME:
            case TIMESTAMP:
                return DataTypes.TIMESTAMP().getLogicalType();
                // Semi-structured
            case JSON:
                return DataTypes.STRING().getLogicalType();
            case ARRAY:
            case MAP:
            case STRUCT:
            case HLL:
            case BITMAP:
            case QUANTILE_STATE:
                return DataTypes.STRING().getLogicalType();
            default:
                return DataTypes.STRING().getLogicalType();
        }
    }
}

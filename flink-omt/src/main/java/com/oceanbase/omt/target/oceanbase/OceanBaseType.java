package com.oceanbase.omt.target.oceanbase;

import static com.oceanbase.omt.source.starrocks.StarRocksType.extractBaseType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.oceanbase.omt.catalog.OceanBaseColumn;

/**
 * @author yixing
 */
public class OceanBaseType {
    // Numeric
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String LARGEINT = "LARGEINT";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";

    // String
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String BINARY = "BINARY";
    public static final String VARBINARY = "VARBINARY";

    // Date
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";

    // Semi-structured
    public static final String JSON = "JSON";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";


    public static LogicalType toFlinkDataType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString().toUpperCase()) {
            // Numeric
            case BOOLEAN:
                return new BooleanType();
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
                return DataTypes.STRING().getLogicalType();
            case BINARY:
                return DataTypes.BINARY(fieldSchema.getColumnSize()).getLogicalType();
            case VARBINARY:
                return DataTypes.VARBINARY(fieldSchema.getColumnSize()).getLogicalType();

            // Date
            case DATE:
                return DataTypes.DATE().getLogicalType();
            case DATETIME:
                // Since StarRocks cannot obtain precision information
                return DataTypes.TIMESTAMP().getLogicalType();

            // Semi-structured
            case JSON:
                return DataTypes.STRING().getLogicalType();
            case ARRAY:
                return parseArrayType(fieldSchema.getColumnType().toUpperCase());
            case STRUCT:
                return DataTypes.ROW().getLogicalType();
            case MAP:
                String columnType = fieldSchema.getColumnType().toLowerCase();
                String[] split = columnType.substring(4, columnType.length() - 1).split(",");
                List<String> list =
                        Arrays.stream(split)
                                .map(sp -> sp.contains("(") ? sp.substring(0, sp.indexOf("(")) : sp)
                                .collect(Collectors.toList());
                OceanBaseColumn column1 =
                        OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                .withTypeString(list.get(0))
                                .build();
                OceanBaseColumn column2 =
                        OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                .withTypeString(list.get(1))
                                .build();
                return new MapType(toFlinkDataType(column1), toFlinkDataType(column2));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported StarRocks type: " + fieldSchema.getTypeString());
        }
    }
    public static LogicalType parseArrayType(String type) {
        if (!type.startsWith("ARRAY<") || !type.endsWith(">")) {
            throw new IllegalArgumentException(
                    "The provided type is not a valid ARRAY type: " + type);
        }
        int count = 0;
        String innerTypeString = type.substring(6, type.length() - 1).trim();
        while (innerTypeString.startsWith("ARRAY<") && innerTypeString.endsWith(">")) {
            innerTypeString = innerTypeString.substring(6, innerTypeString.length() - 1).trim();
            count++;
        }
        OceanBaseColumn oceanBaseColumn = new OceanBaseColumn();
        oceanBaseColumn.setTypeString(extractBaseType(innerTypeString));
        LogicalType dataType = toFlinkDataType(oceanBaseColumn);
        for (int i = 0; i <= count; i++) {
            dataType = new ArrayType(dataType);
        }
        return dataType;
    }
}

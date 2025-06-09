package com.oceanbase.omt.source.clickhouse;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.Preconditions;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseMySQLType;
import com.oceanbase.omt.source.starrocks.StarRocksType;

/**
 * @author yixing
 */
public class ClickHouseType {
    private static final String DOUBLE_PRECISION_FORMAT = "%s(%s,%s)";
    public static final String INT32 = "Int32";
    public static final String FLOAT64= "Float64";
    public static final String UINTt64 = "UInt64";
    public static final String UINT32 = "UInt32";
    public static final String DECIMAL = "Decimal";

    public static final String LOWCARDINALITY = "LowCardinality";

    public static final String BOOLEAN = "LowCardinality";

    // String

    public static final String STRING = "String";


    // Date
    public static final String DATE = "Date";

    public static final String DATETIME = "DateTime";



    // Semi-structured


    // Other


    public static String toOceanBaseMySQLType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString()) {
            // Numeric
            case INT32:
                return OceanBaseMySQLType.INT;
            case FLOAT64:
                return OceanBaseMySQLType.FLOAT;
            case UINTt64:
                return OceanBaseMySQLType.BIGINT;
            case UINT32:
                return OceanBaseMySQLType.INT;
            case DECIMAL:
                return String.format(
                        DOUBLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.DECIMAL,
                        fieldSchema.getColumnSize(),
                        fieldSchema.getNumericScale());
            case STRING:
            case LOWCARDINALITY:
                return OceanBaseMySQLType.TEXT;
            case DATE:
                return OceanBaseMySQLType.DATE;
            case DATETIME:
                return OceanBaseMySQLType.DATETIME;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported ClickHouse type: " + fieldSchema.getTypeString());
        }
    }



    public static List<String> getDateBasedType() {
        return Arrays.asList(ClickHouseType.DATE, ClickHouseType.DATETIME);
    }
}

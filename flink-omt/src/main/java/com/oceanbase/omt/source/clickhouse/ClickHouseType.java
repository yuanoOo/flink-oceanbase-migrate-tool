package com.oceanbase.omt.source.clickhouse;

import static org.apache.flink.table.types.logical.DecimalType.MAX_PRECISION;

import java.util.Arrays;
import java.util.List;

import java.util.Objects;
import java.util.Stack;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseMySQLType;


/**
 * @author yixing
 */
public class ClickHouseType {
    private static final String DOUBLE_PRECISION_FORMAT = "%s(%s,%s)";


    private static final String SINGLE_PRECISION_FORMAT = "%s(%s)";

    public static final String Int8="Int8";

    public static final String Bool="Bool";
    public static final String Int16="Int16";
    public static final String UInt8="UInt8";
    public static final String Int32="Int32";
    public static final String UInt16="UInt16";
    public static final String IntervalYear="IntervalYear";
    public static final String IntervalMonth="IntervalMonth";
    public static final String IntervalWeek="IntervalWeek";
    public static final String IntervalDay="IntervalDay";
    public static final String IntervalHour="IntervalHour";
    public static final String IntervalQuarter="IntervalQuarter";
    public static final String IntervalMinute="IntervalMinute";
    public static final String IntervalSecond="IntervalSecond";
    public static final String Int64="Int64";
    public static final String UInt32="UInt32";
    public static final String Int128="Int128";
    public static final String Int256="Int256";
    public static final String UInt64="UInt64";
    public static final String UInt128="UInt128";
    public static final String UInt256="UInt256";
    public static final String Float32="Float32";
    public static final String Float64="Float64";
    public static final String Decimal="Decimal";
    public static final String Decimal32="Decimal32";
    public static final String Decimal64="Decimal64";
    public static final String String="String";
    public static final String Enum8="Enum8";
    public static final String Decimal128="Decimal128";
    public static final String Decimal256="Decimal256";
    public static final String Enum16="Enum16";
    public static final String FixedString="FixedString";
    public static final String IPv4="IPv4";
    public static final String IPv6="IPv6";
    public static final String UUID="UUID";
    public static final String Date="Date";
    public static final String Date32="Date32";
    public static final String DateTime="DateTime";
    public static final String DateTime32="DateTime32";
    public static final String DateTime64="DateTime64";
    public static final String Array="Array";
    public static final String Map="Map";
    public static final String Tuple="Tuple";
    public static final String Nested="Nested";
    public static final String AggregateFunction="AggregateFunction";



    public static String toOceanBaseMySQLType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString()) {
            case Int8:
                return OceanBaseMySQLType.TINYINT;
            case Bool:
                return OceanBaseMySQLType.BOOLEAN;
            case Int16:
            case UInt8:
                return OceanBaseMySQLType.SMALLINT;
            case Int32:
            case UInt16:
            case IntervalYear:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalQuarter:
            case IntervalMinute:
            case IntervalSecond:
                return OceanBaseMySQLType.INT;
            case Int64:
            case UInt32:
                return OceanBaseMySQLType.BIGINT;
            case Int128:
            case Int256:
            case UInt64:
            case UInt128:
            case UInt256:
                return java.lang.String.format(
                        DOUBLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.DECIMAL,
                        MAX_PRECISION,
                        0);
            case Float32:
                return OceanBaseMySQLType.FLOAT;
            case Float64:
                return OceanBaseMySQLType.DOUBLE;
            case Decimal:
                return java.lang.String.format(
                        DOUBLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.DECIMAL,
                        fieldSchema.getColumnSize(),
                        fieldSchema.getNumericScale());
            case String:
            case Enum8:
            case Enum16:
                return String.format(
                        SINGLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.VARCHAR,
                        OceanBaseMySQLType.RE_VARCHAR_SIZE);
            case FixedString:
            case IPv4:
            case IPv6:
            case UUID:
                Integer sRVarColumnSize = fieldSchema.getColumnSize();
                Preconditions.checkState(Objects.nonNull(sRVarColumnSize));
                if (sRVarColumnSize > OceanBaseMySQLType.MAX_VARCHAR_SIZE) {
                    return OceanBaseMySQLType.MEDIUMTEXT;
                }
                return String.format(
                        SINGLE_PRECISION_FORMAT, OceanBaseMySQLType.VARCHAR, sRVarColumnSize);
            case Date:
                return OceanBaseMySQLType.DATE;
            case DateTime:
            case DateTime32:
            case DateTime64:
                return OceanBaseMySQLType.TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported ClickHouse type: " + fieldSchema.getTypeString());
        }
    }

    public static LogicalType toFlinkType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString()) {
            case Int8:
                return DataTypes.TINYINT().getLogicalType();
            case Bool:
                return DataTypes.BOOLEAN().getLogicalType();
            case Int16:
            case UInt8:
                return DataTypes.SMALLINT().getLogicalType();
            case Int32:
            case UInt16:
            case IntervalYear:
            case IntervalMonth:
            case IntervalWeek:
            case IntervalDay:
            case IntervalHour:
            case IntervalQuarter:
            case IntervalMinute:
            case IntervalSecond:
                return DataTypes.INT().getLogicalType();
            case Int64:
            case UInt32:
                return DataTypes.BIGINT().getLogicalType();
            case Int128:
            case Int256:
            case UInt64:
            case UInt128:
            case UInt256:
                return DataTypes.DECIMAL(MAX_PRECISION, 0).getLogicalType();
            case Float32:
                return DataTypes.FLOAT().getLogicalType();
            case Float64:
                return DataTypes.DOUBLE().getLogicalType();
            case Decimal:
                return DataTypes.DECIMAL(
                        fieldSchema.getColumnSize(), fieldSchema.getNumericScale()).getLogicalType();
            case Decimal32:
                return DataTypes.DECIMAL(9, fieldSchema.getNumericScale()).getLogicalType();
            case Decimal64:
                return DataTypes.DECIMAL(18, fieldSchema.getNumericScale()).getLogicalType();
            case Decimal128:
            case Decimal256:
                return DataTypes.DECIMAL(
                        Math.min(MAX_PRECISION, fieldSchema.getColumnSize()),
                        Math.min(MAX_PRECISION, fieldSchema.getNumericScale())).getLogicalType();
            case String:
            case Enum8:
            case Enum16:
                return DataTypes.STRING().getLogicalType();
            case FixedString:
            case IPv4:
            case IPv6:
            case UUID:
                return DataTypes.VARCHAR(fieldSchema.getColumnSize()).getLogicalType();
            case Date:
            case Date32:
                return DataTypes.DATE().getLogicalType();
            case DateTime:
            case DateTime32:
            case DateTime64:
                return DataTypes.TIMESTAMP(fieldSchema.getNumericScale()).getLogicalType();
            case Array:
                //java.lang.String columnType = fieldSchema.getColumnType();
                //java.lang.String s = extractOuterParensContent(columnType);
                //
                //return DataTypes.ARRAY();
            case Map:
                //return DataTypes.MAP(
                //        toFlinkType(clickHouseColumnInfo.getKeyInfo()),
                //        toFlinkType(clickHouseColumnInfo.getValueInfo()));
            case Tuple:
                //return DataTypes.ROW(
                //        clickHouseColumnInfo.getNestedColumns().stream()
                //                .map((col) -> new Tuple2<>(col, toFlinkType(col)))
                //                .map(tuple -> DataTypes.FIELD(tuple.f0.getColumnName(), tuple.f1))
                //                .collect(Collectors.toList()));
            case Nested:
            case AggregateFunction:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type:" + fieldSchema.getTypeString());
        }
    }
}

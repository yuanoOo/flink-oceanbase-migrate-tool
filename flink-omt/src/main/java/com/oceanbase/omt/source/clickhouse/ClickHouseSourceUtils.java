package com.oceanbase.omt.source.clickhouse;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.ClickHouseBatchInputFormat;
import org.apache.flink.connector.clickhouse.internal.ClickHouseShardInputFormat;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class ClickHouseSourceUtils {
    public static InputFormatSourceFunction<RowData> createClickHouseSource(
            ClickHouseReadOptions readOptions,
            Properties connectionProperties,
            String[] fieldNames,
            DataType[] fieldTypes,
            TypeInformation<RowData> rowTypeInfo) {

        // 构建输入格式
        AbstractClickHouseInputFormat inputFormat = new ClickHouseBatchInputFormat.Builder()
                .withOptions(readOptions)
                .withConnectionProperties(connectionProperties)
                .withFieldNames(fieldNames)
                .withFieldTypes(fieldTypes)
                .withRowDataTypeInfo(rowTypeInfo)
                .build();

        return new InputFormatSourceFunction<>(inputFormat, rowTypeInfo);
    }
}

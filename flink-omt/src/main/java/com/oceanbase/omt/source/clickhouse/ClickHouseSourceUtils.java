package com.oceanbase.omt.source.clickhouse;


import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.ClickHouseBatchInputFormat;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;


public class ClickHouseSourceUtils {
    public static InputFormatSourceFunction<RowData> createClickHouseSource(
            ClickHouseReadOptions readOptions,
            Properties connectionProperties,
            String[] fieldNames,
            DataType[] fieldTypes,
            TypeInformation<RowData> rowTypeInfo, LogicalType[] logicalTypes) {

        ClickHouseConnectionProvider clickHouseConnectionProvider = new ClickHouseConnectionProvider(readOptions, connectionProperties);
        ClickHouseRowConverter clickHouseRowConverter = new ClickHouseRowConverter(RowType.of(logicalTypes));

        AbstractClickHouseInputFormat inputFormat = new ClickHouseBatchInputFormat(clickHouseConnectionProvider
                ,clickHouseRowConverter,readOptions,fieldNames,rowTypeInfo,null,"","",-1L);
        return new InputFormatSourceFunction<>(inputFormat, rowTypeInfo);
    }
}

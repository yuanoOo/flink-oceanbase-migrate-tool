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

import java.util.Properties;

public class ClickHouseSourceUtils {
    public static InputFormatSourceFunction<RowData> createClickHouseSource(
            ClickHouseReadOptions readOptions,
            Properties connectionProperties,
            String[] fieldNames,
            DataType[] fieldTypes,
            TypeInformation<RowData> rowTypeInfo,
            LogicalType[] logicalTypes) {

        ClickHouseConnectionProvider clickHouseConnectionProvider =
                new ClickHouseConnectionProvider(readOptions, connectionProperties);
        ClickHouseRowConverter clickHouseRowConverter =
                new ClickHouseRowConverter(RowType.of(logicalTypes));

        AbstractClickHouseInputFormat inputFormat =
                new ClickHouseBatchInputFormat(
                        clickHouseConnectionProvider,
                        clickHouseRowConverter,
                        readOptions,
                        fieldNames,
                        rowTypeInfo,
                        null,
                        "",
                        "",
                        -1L);
        return new InputFormatSourceFunction<>(inputFormat, rowTypeInfo);
    }
}

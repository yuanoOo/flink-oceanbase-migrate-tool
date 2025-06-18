package com.oceanbase.omt.source.clickhouse;

import org.apache.flink.table.types.logical.LogicalType;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.source.TypeConverter;

public class ClickHouseTypeConverter implements TypeConverter {

    @Override
    public LogicalType convert(OceanBaseColumn oceanBaseColumn) {
        return ClickHouseType.toFlinkType(oceanBaseColumn);
    }
}

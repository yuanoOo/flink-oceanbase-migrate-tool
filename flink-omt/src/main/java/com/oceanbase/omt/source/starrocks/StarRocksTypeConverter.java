package com.oceanbase.omt.source.starrocks;

import org.apache.flink.table.types.logical.LogicalType;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.source.TypeConverter;

public class StarRocksTypeConverter implements TypeConverter {
    @Override
    public LogicalType convert(OceanBaseColumn oceanBaseColumn) {
        return StarRocksType.toFlinkDataType(oceanBaseColumn);
    }
}

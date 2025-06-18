package com.oceanbase.omt.source;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import org.apache.flink.table.types.logical.LogicalType;
/**
 * @author yixing
 */
public interface TypeConverter {
    LogicalType convert(OceanBaseColumn oceanBaseColumn);
}

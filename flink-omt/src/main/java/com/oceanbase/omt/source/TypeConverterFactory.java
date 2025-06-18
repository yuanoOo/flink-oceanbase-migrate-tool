package com.oceanbase.omt.source;

import java.util.HashMap;
import java.util.Map;

import com.oceanbase.omt.source.clickhouse.ClickHouseTypeConverter;
import com.oceanbase.omt.source.starrocks.StarRocksTypeConverter;

public class TypeConverterFactory {
    private static final Map<DataSourceType, TypeConverter> converters = new HashMap<>();

    static {
        converters.put(DataSourceType.STAR_ROCKS, new StarRocksTypeConverter());
        converters.put(DataSourceType.CLICK_HOUSE, new ClickHouseTypeConverter());
        // 添加更多数据源支持
    }

    public static TypeConverter getConverter(DataSourceType type) {
        return converters.get(type);
    }
}

package com.oceanbase.omt.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import lombok.Getter;

@Getter
public enum DataSourceType {
    STAR_ROCKS("StarRocks"),
    CLICK_HOUSE("ClickHouse");

    private String value;

    DataSourceType(String value) {
        this.value = value;
    }

    private static final Map<String, DataSourceType> map = new HashMap<>();

    public static final Function<String, DataSourceType> FROM_VALUE = v -> {
        return Arrays.stream(values())
                .filter(t -> t.getValue().equals(v))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown value: " + v));
    };
}

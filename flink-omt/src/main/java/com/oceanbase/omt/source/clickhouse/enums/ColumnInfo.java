package com.oceanbase.omt.source.clickhouse.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author yixing
 */
@Getter
@AllArgsConstructor
public enum ColumnInfo {
    NAME("name"),
    TYPE("type"),
    DEFAULT_KIND("default_kind"),
    DEFAULT_EXPRESSION("default_expression"),
    IS_NULLABLE("is_nullable"),
    COMMENT("comment"),
    NUMERIC_PRECISION("numeric_precision"),
    NUMERIC_SCALE("numeric_scale"),
    IS_IN_PARTITION_KEY("is_in_partition_key"),
    IS_IN_PRIMARY_KEY("is_in_primary_key"),
    IS_IN_SORTING_KEY("is_in_sorting_key"),
    IS_IN_SAMPLING_KEY("is_in_sampling_key");
    private final String name;
}

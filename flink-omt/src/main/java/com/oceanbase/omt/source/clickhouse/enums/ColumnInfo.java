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
    NUMERIC_SCALE("numeric_scale");
    private final String name;
}

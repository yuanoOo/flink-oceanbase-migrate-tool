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
package com.oceanbase.omt.source.clickhouse.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** @author yixing */
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
    IS_IN_SAMPLING_KEY("is_in_sampling_key"),
    DATETIME_PRECISION("datetime_precision"),
    CHARACTER_OCTET_LENGTH("character_octet_length");
    private final String name;
}

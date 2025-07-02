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
package com.oceanbase.omt.utils;

import com.oceanbase.omt.source.clickhouse.ClickHouseType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;

public class DateFormatConverter {
    private static final int YEAR_LENGTH = 4;
    private static final int YEAR_MONTH_LENGTH = 6;
    private static final int DATE_LENGTH = 8;

    public static String convertYearToYYYYMMDD(String yearStr, String type) {
        if (type.equals(ClickHouseType.Date) || type.equals(ClickHouseType.Date32)) {
            int year = Integer.parseInt(yearStr);
            LocalDate date = Year.of(year).plusYears(1).atDay(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else {
            int year = Integer.parseInt(yearStr);
            LocalDateTime date = Year.of(year).atDay(1).plusYears(1).atStartOfDay();
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
    }

    public static String convertYearMonthToStandard(String yearMonthStr, String type) {
        if (type.equals(ClickHouseType.Date) || type.equals(ClickHouseType.Date32)) {
            int year = Integer.parseInt(yearMonthStr.substring(0, 4));
            int month = Integer.parseInt(yearMonthStr.substring(4, 6));
            LocalDate date = Year.of(year).atMonth(month).plusMonths(1).atDay(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else {
            int year = Integer.parseInt(yearMonthStr.substring(0, 4));
            int month = Integer.parseInt(yearMonthStr.substring(4, 6));
            LocalDateTime date = Year.of(year).atMonth(month).plusMonths(1).atDay(1).atStartOfDay();
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
    }

    public static String convertDateToStandardFormat(String dateStr, String type) {
        if (type.equals(ClickHouseType.Date) || type.equals(ClickHouseType.Date32)) {
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.BASIC_ISO_DATE).plusDays(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else {
            LocalDateTime date =
                    LocalDateTime.parse(dateStr, DateTimeFormatter.BASIC_ISO_DATE).plusDays(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
    }

    public static String convertPartitionKey(String partitionKey, String type) {
        switch (partitionKey.length()) {
            case YEAR_LENGTH:
                return convertYearToYYYYMMDD(partitionKey, type);
            case YEAR_MONTH_LENGTH:
                return convertYearMonthToStandard(partitionKey, type);
            case DATE_LENGTH:
                return convertDateToStandardFormat(partitionKey, type);
            default:
                throw new IllegalArgumentException("Invalid partition key format");
        }
    }
}

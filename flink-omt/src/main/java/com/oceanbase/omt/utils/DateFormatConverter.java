package com.oceanbase.omt.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

import com.oceanbase.omt.source.clickhouse.ClickHouseType;

public class DateFormatConverter {
    private static final int YEAR_LENGTH = 4;
    private static final int YEAR_MONTH_LENGTH = 6;
    private static final int DATE_LENGTH = 8;

    // "2025" -> "2025/01/01"
    public static String convertYearToYYYYMMDD(String yearStr,String type) {
        if (type.equals(ClickHouseType.Date)){
            int year = Integer.parseInt(yearStr);
            LocalDate date = Year.of(year).plusYears(1).atDay(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }else {
            int year = Integer.parseInt(yearStr);
            LocalDateTime date = Year.of(year).atDay(1).plusYears(1).atStartOfDay();
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

    }

    // "202506" -> "2025/06/01"
    public static String convertYearMonthToStandard(String yearMonthStr,String type) {
        if (type.equals(ClickHouseType.Date)){
            int year = Integer.parseInt(yearMonthStr.substring(0, 4));
            int month = Integer.parseInt(yearMonthStr.substring(4, 6));
            LocalDate date = Year.of(year).atMonth(month).plusMonths(1).atDay(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        }else {
            int year = Integer.parseInt(yearMonthStr.substring(0, 4));
            int month = Integer.parseInt(yearMonthStr.substring(4, 6));
            LocalDateTime date = Year.of(year).atMonth(month).plusMonths(1).atDay(1).atStartOfDay();
            return date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }

    }

    // "20250101" -> "2025/01/01"
    public static String convertDateToStandardFormat(String dateStr,String type) {
        if (type.equals(ClickHouseType.Date)){
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.BASIC_ISO_DATE).plusDays(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        }else {
            LocalDateTime date = LocalDateTime.parse(dateStr, DateTimeFormatter.BASIC_ISO_DATE).plusDays(1);
            return date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"));
        }

    }

    public static String convertPartitionKey(String partitionKey,String type) {
        switch (partitionKey.length()) {
            case YEAR_LENGTH:
                return convertYearToYYYYMMDD(partitionKey,type);
            case YEAR_MONTH_LENGTH:
                return convertYearMonthToStandard(partitionKey,type);
            case DATE_LENGTH:
                return convertDateToStandardFormat(partitionKey,type);
            default:
                throw new IllegalArgumentException("Invalid partition key format");
        }
    }



}

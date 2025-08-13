package com.oceanbase.omt.source.doris;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.source.TypeConverter;

import org.apache.flink.table.types.logical.LogicalType;

public class DorisTypeConverter implements TypeConverter {

    @Override
    public LogicalType convert(OceanBaseColumn oceanBaseColumn) {
        return DorisType.toFlinkDataType(oceanBaseColumn);
    }

    public static String convertToOceanBaseType(String sourceType) {
        if (sourceType == null) {
            return null;
        }

        String upperType = sourceType.toUpperCase();

        if (upperType.startsWith("TINYINT")) {
            return "TINYINT";
        } else if (upperType.startsWith("SMALLINT")) {
            return "SMALLINT";
        } else if (upperType.startsWith("INT")) {
            return "INT";
        } else if (upperType.startsWith("BIGINT")) {
            return "BIGINT";
        } else if (upperType.startsWith("LARGEINT")) {
            return "BIGINT";
        } else if (upperType.startsWith("FLOAT")) {
            return "FLOAT";
        } else if (upperType.startsWith("DOUBLE")) {
            return "DOUBLE";
        } else if (upperType.startsWith("DECIMAL")) {
            return sourceType;
        } else if (upperType.startsWith("BOOLEAN")) {
            return "BOOLEAN";
        } else if (upperType.startsWith("STRING")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("VARCHAR")) {
            return sourceType;
        } else if (upperType.startsWith("CHAR")) {
            return sourceType;
        } else if (upperType.startsWith("TEXT")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("DATETIME") || upperType.startsWith("TIMESTAMP")) {
            return "DATETIME";
        } else if (upperType.startsWith("DATE")) {
            return "DATE";
        } else if (upperType.startsWith("JSON")) {
            return "JSON";
        } else if (upperType.startsWith("ARRAY")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("MAP")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("STRUCT")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("BITMAP")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("HLL")) {
            return "VARCHAR(65535)";
        } else if (upperType.startsWith("QUANTILE_STATE")) {
            return "VARCHAR(65535)";
        }

        return "VARCHAR(65535)";
    }
}

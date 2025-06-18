package com.oceanbase.omt.source.clickhouse;

public interface ClickHouseConfig {
    String JDBC_URL = "jdbc-url";
    String USERNAME = "username";
    String PASSWORD = "password";
    String TABLE_NAME = "database-name";
    String DATABASE_NAME = "table-name";
    String SCAN_URL = "scan-url";
    String SCAN_COLUMNS = "scan.columns";
    String SCAN_FILTER = "scan.filter";
}

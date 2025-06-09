package com.oceanbase.omt.source.clickhouse.connect;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.apache.flink.shaded.clickhouse.ru.yandex.clickhouse.ClickHouseConnection;
import org.apache.flink.shaded.clickhouse.ru.yandex.clickhouse.ClickHouseDataSource;
import org.apache.flink.shaded.clickhouse.ru.yandex.clickhouse.settings.ClickHouseProperties;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class ClickHouseSource implements SourceFunction<RowData> {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String query;
    private volatile boolean isRunning = true;

    public ClickHouseSource(String jdbcUrl, String username, String password, String query) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.query = query;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser(username);
        props.setPassword(password);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(jdbcUrl, props);

        try (ClickHouseConnection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (isRunning && rs.next()) {
                GenericRowData row = new GenericRowData(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    // 根据列类型转换为 RowData 支持的类型
                    if (value instanceof String) {
                        row.setField(i - 1, StringData.fromString((String) value));
                    } else if (value instanceof Integer) {
                        row.setField(i - 1, value);
                    } else if (value instanceof Long) {
                        row.setField(i - 1, value);
                    } // 添加其他类型转换
                }
                ctx.collect(row);
            }
        }
    }

    @Override
    public void cancel() {

    }
}

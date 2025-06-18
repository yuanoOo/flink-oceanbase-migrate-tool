package com.oceanbase.omt.source.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oceanbase.omt.partition.PartitionInfo;
import com.oceanbase.omt.utils.DataSourceUtils;

/**
 * @author yixing
 */
public class ClickHouseJdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseJdbcUtils.class);

    public static Connection getConnection(String url,String username,String password){
        try {
            return DataSourceUtils.getSourceDataSource(url,username,password).getConnection();
        } catch (SQLException e) {
            LOG.error("Failed to get connection", e);
            throw new RuntimeException("Failed to get connection", e);
        }
    }



    public static List<Tuple2<String, String>> executeDoubleColumnStatement(
            Connection connection, String sql, String... params) {
        try (Connection conn = connection;
                PreparedStatement statement = conn.prepareStatement(sql)) {
            List<Tuple2<String, String>> columnValues = new ArrayList<>();
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            LOG.info(statement.toString());
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnValue1 = rs.getString(1);
                    String columnValue2 = rs.getString(2);
                    columnValues.add(Tuple2.of(columnValue1, columnValue2));
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to execute sql: %s", sql), e);
        }
    }

    public static List<PartitionInfo> obtainPartitionInfo(Connection connection, String databaseName, String tableName){
        String sql = "SELECT *\n"
                     + "FROM system.parts\n"
                     + "WHERE database = ? AND table = ?";
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, databaseName);
            statement.setObject(2, tableName);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()){
                String partitionName = resultSet.getString("partition");
                if (!partitionName.equals("tuple()")){
                    PartitionInfo partitionInfo = new PartitionInfo();
                    partitionInfo.withPartitionName(partitionName);
                    partitionInfos.add(partitionInfo);
                }
            }
        }catch (SQLException e){
            LOG.error("Failed to execute sql: {}", sql, e);
            throw new RuntimeException(String.format("Failed to execute sql: %s", sql), e);
        }
        return partitionInfos;
    }

}

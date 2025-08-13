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
package com.oceanbase.omt.source.doris;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.partition.PartitionInfo;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Doris JDBC工具类，提供Doris数据库连接和查询相关的工具方法 */
public class DorisJdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DorisJdbcUtils.class);

    /**
     * 执行双列查询语句，用于获取分区信息
     *
     * @param connection 数据库连接
     * @param sql SQL语句
     * @param params 查询参数
     * @return 查询结果列表
     * @throws SQLException SQL异常
     */
    public static List<Tuple2<String, String>> executeDoubleColumnStatement(
            Connection connection, String sql, Object... params) throws SQLException {
        List<Tuple2<String, String>> results = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String value1 = resultSet.getString(1);
                    String value2 = resultSet.getString(2);
                    if (value1 != null && value2 != null) {
                        results.add(Tuple2.of(value1, value2));
                    }
                }
            }
        }
        return results;
    }

    /**
     * 获取表的分区信息
     *
     * @param connection 数据库连接
     * @param databaseName 数据库名
     * @param tableName 表名
     * @param columns 表列信息
     * @return 分区信息列表
     * @throws SQLException SQL异常
     */
    public static List<PartitionInfo> obtainPartitionInfo(
            Connection connection,
            String databaseName,
            String tableName,
            List<OceanBaseColumn> columns)
            throws SQLException {
        List<PartitionInfo> partitionInfos = new ArrayList<>();

        // 查询表的分区信息
        String partitionQuery =
                "SELECT PARTITION_NAME, PARTITION_DESCRIPTION, PARTITION_ORDINAL_POSITION "
                        + "FROM INFORMATION_SCHEMA.PARTITIONS "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                        + "ORDER BY PARTITION_ORDINAL_POSITION";

        try (PreparedStatement statement = connection.prepareStatement(partitionQuery)) {
            statement.setString(1, databaseName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String partitionName = resultSet.getString("PARTITION_NAME");
                    String partitionDescription = resultSet.getString("PARTITION_DESCRIPTION");
                    int ordinalPosition = resultSet.getInt("PARTITION_ORDINAL_POSITION");

                    if (partitionName != null && !partitionName.isEmpty()) {
                        PartitionInfo partitionInfo = new PartitionInfo();
                        partitionInfo.withPartitionName(partitionName);
                        // 注意：PartitionInfo类没有setPartitionDescription和setOrdinalPosition方法
                        // 这里暂时跳过这些字段的设置
                        partitionInfos.add(partitionInfo);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to get partition info for table {}.{}: {}",
                    databaseName,
                    tableName,
                    e.getMessage());
            // 如果查询分区信息失败，返回空列表，不影响主流程
        }

        return partitionInfos;
    }

    /**
     * 检查表是否存在
     *
     * @param connection 数据库连接
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return 表是否存在
     * @throws SQLException SQL异常
     */
    public static boolean tableExists(Connection connection, String databaseName, String tableName)
            throws SQLException {
        String checkTableSql =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

        try (PreparedStatement statement = connection.prepareStatement(checkTableSql)) {
            statement.setString(1, databaseName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    /**
     * 检查数据库是否存在
     *
     * @param connection 数据库连接
     * @param databaseName 数据库名
     * @return 数据库是否存在
     * @throws SQLException SQL异常
     */
    public static boolean databaseExists(Connection connection, String databaseName)
            throws SQLException {
        String checkDatabaseSql =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA " + "WHERE SCHEMA_NAME = ?";

        try (PreparedStatement statement = connection.prepareStatement(checkDatabaseSql)) {
            statement.setString(1, databaseName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        }
        return false;
    }
}

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

package com.oceanbase.omt.source.clickhouse;

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

/** @author yixing */
public class ClickHouseJdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseJdbcUtils.class);

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

    public static List<PartitionInfo> obtainPartitionInfo(
            Connection connection, String databaseName, String tableName) {
        String sql =
                "SELECT *\n"
                        + "FROM system.parts\n"
                        + "WHERE database = ? AND table = ? and active = 1";
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, databaseName);
            statement.setObject(2, tableName);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String partitionName = resultSet.getString("partition");
                if (!partitionName.equals("tuple()")) {
                    PartitionInfo partitionInfo = new PartitionInfo();
                    if (partitionInfos.stream()
                            .map(PartitionInfo::getPartitionName)
                            .noneMatch(p -> p.equals(partitionName))) {
                        partitionInfo.withPartitionName(partitionName);
                        partitionInfos.add(partitionInfo);
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to execute sql: {}", sql, e);
            throw new RuntimeException(String.format("Failed to execute sql: %s", sql), e);
        }
        return partitionInfos;
    }
}

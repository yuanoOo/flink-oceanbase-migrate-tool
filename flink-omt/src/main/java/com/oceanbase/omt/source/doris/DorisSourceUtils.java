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

import com.oceanbase.omt.parser.SourceMigrateConfig;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Doris数据源工具类，提供Doris数据源相关的工具方法 */
public class DorisSourceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceUtils.class);

    /**
     * 创建Doris数据源
     *
     * @param sourceConfig 源配置
     * @param tableSchema 表结构
     * @return Doris数据源
     */
    public static SourceFunction<RowData> createDorisSource(
            SourceMigrateConfig sourceConfig, TableSchema tableSchema) {

        // 构建Doris连接配置
        Map<String, String> dorisConfig = buildDorisConfig(sourceConfig);

        LOG.info("Creating Doris source with config: {}", dorisConfig);

        // TODO: 实现真实的Doris Source创建逻辑
        // 这里需要根据Doris Flink Connector的实际API来实现
        // 目前使用占位符实现，实际使用时需要替换为真实的Doris Source创建逻辑
        LOG.info("Creating Doris source with config: {}", dorisConfig);
        throw new UnsupportedOperationException(
                "Doris source creation is not implemented yet. "
                        + "Please implement according to Doris Flink Connector API.");
    }

    /**
     * 构建Doris连接配置
     *
     * @param sourceConfig 源配置
     * @return Doris配置映射
     */
    public static Map<String, String> buildDorisConfig(SourceMigrateConfig sourceConfig) {
        Map<String, String> config = new HashMap<>();
        Map<String, String> other = sourceConfig.getOther();

        // 基本连接配置
        config.put("scan-url", other.get(DorisConfig.SCAN_URL));
        config.put("jdbc-url", other.get(DorisConfig.JDBC_URL));
        config.put("username", other.get(DorisConfig.USERNAME));
        config.put("password", other.get(DorisConfig.PASSWORD));
        config.put("database-name", other.get(DorisConfig.DATABASE_NAME));
        config.put("table-name", other.get(DorisConfig.TABLE_NAME));

        // 可选配置参数
        String scanConnectTimeoutMs = other.get(DorisConfig.SCAN_CONNECT_TIMEOUT_MS);
        if (scanConnectTimeoutMs != null) {
            config.put("scan.connect.timeout-ms", scanConnectTimeoutMs);
        }
        String scanParamsKeepAliveMin = other.get(DorisConfig.SCAN_PARAMS_KEEP_ALIVE_MIN);
        if (scanParamsKeepAliveMin != null) {
            config.put("scan.params.keep-alive-min", scanParamsKeepAliveMin);
        }
        String scanParamsQueryTimeoutS = other.get(DorisConfig.SCAN_PARAMS_QUERY_TIMEOUT_S);
        if (scanParamsQueryTimeoutS != null) {
            config.put("scan.params.query-timeout-s", scanParamsQueryTimeoutS);
        }
        String scanParamsMemLimitByte = other.get(DorisConfig.SCAN_PARAMS_MEM_LIMIT_BYTE);
        if (scanParamsMemLimitByte != null) {
            config.put("scan.params.mem-limit-byte", scanParamsMemLimitByte);
        }
        String scanMaxRetries = other.get(DorisConfig.SCAN_MAX_RETRIES);
        if (scanMaxRetries != null) {
            config.put("scan.max-retries", scanMaxRetries);
        }

        return config;
    }

    /**
     * 验证Doris配置参数
     *
     * @param sourceConfig 源配置
     * @throws IllegalArgumentException 如果配置无效
     */
    public static void validateDorisConfig(SourceMigrateConfig sourceConfig) {
        Map<String, String> other = sourceConfig.getOther();

        if (other.get(DorisConfig.SCAN_URL) == null || other.get(DorisConfig.SCAN_URL).isEmpty()) {
            throw new IllegalArgumentException("Doris scan-url is required");
        }
        if (other.get(DorisConfig.JDBC_URL) == null || other.get(DorisConfig.JDBC_URL).isEmpty()) {
            throw new IllegalArgumentException("Doris jdbc-url is required");
        }
        if (other.get(DorisConfig.USERNAME) == null || other.get(DorisConfig.USERNAME).isEmpty()) {
            throw new IllegalArgumentException("Doris username is required");
        }
        if (other.get(DorisConfig.PASSWORD) == null || other.get(DorisConfig.PASSWORD).isEmpty()) {
            throw new IllegalArgumentException("Doris password is required");
        }
        if (other.get(DorisConfig.DATABASE_NAME) == null
                || other.get(DorisConfig.DATABASE_NAME).isEmpty()) {
            throw new IllegalArgumentException("Doris database-name is required");
        }
        if (other.get(DorisConfig.TABLE_NAME) == null
                || other.get(DorisConfig.TABLE_NAME).isEmpty()) {
            throw new IllegalArgumentException("Doris table-name is required");
        }

        LOG.info("Doris configuration validation passed");
    }

    /**
     * 获取Doris表的Flink TableSchema
     *
     * @param sourceConfig 源配置
     * @return Flink TableSchema
     */
    public static TableSchema getDorisTableSchema(SourceMigrateConfig sourceConfig) {
        // TODO: 实现从Doris获取表结构的逻辑
        // 这里需要根据实际的Doris Flink Connector API来实现

        Map<String, String> other = sourceConfig.getOther();
        String databaseName = other.get(DorisConfig.DATABASE_NAME);
        String tableName = other.get(DorisConfig.TABLE_NAME);

        LOG.info("Getting Doris table schema for {}.{}", databaseName, tableName);

        // 返回一个默认的TableSchema，实际使用时需要从Doris获取真实的表结构
        return TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("create_time", DataTypes.TIMESTAMP())
                .build();
    }
}

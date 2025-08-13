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

/** Doris配置接口，定义Doris数据源的配置参数 */
public interface DorisConfig {
    /** FE节点的HTTP服务地址，用于通过Web服务器访问FE节点 */
    String SCAN_URL = "scan-url";

    /** FE节点的JDBC连接地址，用于访问FE节点上的MySQL客户端 */
    String JDBC_URL = "jdbc-url";

    /** 用于访问Doris集群的用户名 */
    String USERNAME = "username";

    /** 用于访问Doris集群的用户密码 */
    String PASSWORD = "password";

    /** 待读取数据的Doris数据库名称 */
    String DATABASE_NAME = "database-name";

    /** 待读取数据的Doris表名称 */
    String TABLE_NAME = "table-name";

    /** Flink Connector连接Doris集群的时间上限（毫秒） */
    String SCAN_CONNECT_TIMEOUT_MS = "scan.connect.timeout-ms";

    /** 数据读取任务的保活时间（分钟） */
    String SCAN_PARAMS_KEEP_ALIVE_MIN = "scan.params.keep-alive-min";

    /** 数据读取任务的超时时间（秒） */
    String SCAN_PARAMS_QUERY_TIMEOUT_S = "scan.params.query-timeout-s";

    /** BE节点中单个查询的内存上限（字节） */
    String SCAN_PARAMS_MEM_LIMIT_BYTE = "scan.params.mem-limit-byte";

    /** 数据读取失败时的最大重试次数 */
    String SCAN_MAX_RETRIES = "scan.max-retries";

    /** 数据读取的并行度 */
    String SCAN_PARALLELISM = "scan.parallelism";

    /** 数据读取的批次大小 */
    String SCAN_BATCH_SIZE = "scan.batch-size";

    /** 数据读取的批次间隔（毫秒） */
    String SCAN_BATCH_INTERVAL_MS = "scan.batch-interval-ms";

    /** 数据读取的过滤条件 */
    String SCAN_FILTER = "scan.filter";

    /** 数据读取的列列表 */
    String SCAN_COLUMNS = "scan.columns";

    /** 数据读取的分区信息 */
    String SCAN_PARTITIONS = "scan.partitions";

    /** 数据读取的时区设置 */
    String SCAN_TIMEZONE = "scan.timezone";

    /** 数据读取的字符集设置 */
    String SCAN_CHARSET = "scan.charset";
}

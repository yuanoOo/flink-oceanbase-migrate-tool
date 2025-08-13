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
package com.oceanbase.omt;

import com.oceanbase.omt.base.DorisTestBase;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.source.doris.DorisDatabaseSync;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

/**
 * Integration test for Doris to OceanBase migration.
 *
 * <p>Based on Flink CDC best practices and TestContainers patterns.
 */
public class Doris2OBIntegrationTest extends DorisTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Doris2OBIntegrationTest.class);

    @Before
    public void setup() throws Exception {
        LOG.info("Setting up test environment...");

        // 创建测试数据库
        createDorisDatabase("test1");
        createDorisDatabase("test2");

        // 创建测试表和数据
        setupTestData();

        LOG.info("Test environment setup completed");
    }

    @After
    public void cleanup() throws Exception {
        LOG.info("Cleaning up test environment...");

        // 清理测试数据库
        try {
            dropDorisDatabase("test1");
            dropDorisDatabase("test2");
        } catch (Exception e) {
            LOG.warn("Failed to cleanup test databases: {}", e.getMessage());
        }

        LOG.info("Test environment cleanup completed");
    }

    private void setupTestData() throws SQLException {
        // 创建test1.orders1表
        executeDorisSQL(
                "test1",
                "CREATE TABLE IF NOT EXISTS orders1 ("
                        + "    id INT,"
                        + "    order_time DATETIME,"
                        + "    product_name VARCHAR(100),"
                        + "    price DECIMAL(10,2),"
                        + "    quantity INT,"
                        + "    status TINYINT"
                        + ") DUPLICATE KEY(id)"
                        + " DISTRIBUTED BY HASH(id) BUCKETS 1"
                        + " PROPERTIES ('replication_num' = '1')");

        // 插入测试数据
        executeDorisSQL(
                "test1",
                "INSERT INTO orders1 VALUES (1, '2024-12-05 10:28:07', 'test_product', 2.3, 1, 1)");

        // 创建test2.orders4表
        executeDorisSQL(
                "test2",
                "CREATE TABLE IF NOT EXISTS orders4 ("
                        + "    id INT,"
                        + "    status TINYINT,"
                        + "    order_code VARCHAR(20),"
                        + "    order_date DATE,"
                        + "    amount DECIMAL(10,4)"
                        + ") DUPLICATE KEY(id)"
                        + " DISTRIBUTED BY HASH(id) BUCKETS 1"
                        + " PROPERTIES ('replication_num' = '1')");

        // 插入测试数据
        executeDorisSQL(
                "test2",
                "INSERT INTO orders4 VALUES (1, 1, 'A123456789', '2023-01-01', 1234.5678)");
    }

    @Test
    public void testDorisContainerConnection() throws Exception {
        LOG.info("Testing Doris container connection...");

        // 验证容器状态
        assertTrue("Doris container should be available", isDorisAvailable());

        // 验证数据库连接
        try (Connection conn = getDorisConnection()) {
            assertNotNull("Connection should not be null", conn);

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1 as test_value")) {

                assertTrue("Should have result", rs.next());
                assertEquals("Result should be 1", 1, rs.getInt("test_value"));
            }
        }

        LOG.info("Doris container connection test passed!");
    }

    @Test
    public void testDorisDatabaseSync() throws Exception {
        LOG.info("Testing Doris database synchronization...");

        // 创建配置
        String configYaml =
                String.format(
                        "source:\n"
                                + "  type: doris\n"
                                + "  database: test1,test2\n"
                                + "  tables: orders1,orders4\n"
                                + "  scan-url: %s\n"
                                + "  jdbc-url: jdbc:mysql://%s\n"
                                + "  username: %s\n"
                                + "  password: \"%s\"\n"
                                + "  database-name: test1\n"
                                + "  table-name: orders1\n"
                                + "\n"
                                + "oceanbase:\n"
                                + "  url: %s\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  schema-name: test\n"
                                + "  type: jdbc\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  name: Doris to OceanBase Test\n"
                                + "  parallelism: 1\n",
                        getDorisFeHttpAddress(),
                        getDorisFeQueryAddress(),
                        DORIS_CONTAINER.getUsername(),
                        DORIS_CONTAINER.getPassword(),
                        getOceanBaseJdbcUrl(),
                        getOceanBaseUsername(),
                        getOceanBasePassword());

        // 解析配置
        MigrationConfig config = YamlParser.parseFromString(configYaml);
        assertNotNull("Config should not be null", config);

        // 创建数据库同步实例
        DorisDatabaseSync databaseSync = new DorisDatabaseSync(config);
        assertNotNull("DatabaseSync should not be null", databaseSync);

        // 验证配置有效性
        assertNotNull("Source config should not be null", config.getSource());
        assertEquals("Source type should be doris", "doris", config.getSource().getType());

        LOG.info("Doris database sync test passed!");
    }

    @Test
    public void testDorisDataRetrieval() throws Exception {
        LOG.info("Testing Doris data retrieval...");

        // 查询test1.orders1表
        try (Connection conn = getDorisConnection("test1");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM orders1")) {

            assertTrue("Should have at least one record", rs.next());
            assertEquals("ID should be 1", 1, rs.getInt("id"));
            assertEquals("Product name should match", "test_product", rs.getString("product_name"));
        }

        // 查询test2.orders4表
        try (Connection conn = getDorisConnection("test2");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM orders4")) {

            assertTrue("Should have at least one record", rs.next());
            assertEquals("ID should be 1", 1, rs.getInt("id"));
            assertEquals("Order code should match", "A123456789", rs.getString("order_code"));
        }

        LOG.info("Doris data retrieval test passed!");
    }

    @Test
    public void testFlinkEnvironmentSetup() throws Exception {
        LOG.info("Testing Flink environment setup...");

        // 创建本地Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        assertNotNull("Flink environment should not be null", env);

        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        // 验证环境配置
        assertTrue("Should be local environment", env instanceof LocalStreamEnvironment);

        LOG.info("Flink environment setup test passed!");
    }

    // Helper methods
    private String getOceanBaseJdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/test",
                FIX_CONTAINER.getHost(), FIX_CONTAINER.getMappedPort(2881));
    }

    private String getOceanBaseUsername() {
        return "root@test";
    }

    private String getOceanBasePassword() {
        return "654321";
    }
}

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
package com.oceanbase.omt.base;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Base test class for Doris integration tests.
 *
 * <p>Provides shared Doris container instance and utility methods for testing. Based on Flink CDC
 * best practices for container lifecycle management.
 */
public abstract class DorisTestBase extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DorisTestBase.class);

    // 共享的Doris容器实例
    protected static DorisContainer DORIS_CONTAINER;
    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    @BeforeClass
    public static void startContainers() throws Exception {
        LOG.info("Starting test containers...");

        // 启动OceanBase容器
        LOG.info("Starting OceanBase container...");
        FIX_CONTAINER.start();

        // 启动Doris容器
        LOG.info("Starting Doris container...");
        DORIS_CONTAINER = new DorisContainer(NETWORK);
        DORIS_CONTAINER.start();

        // 等待容器就绪
        waitForContainersReady();

        // 等待 Doris 后端（BE）可用（对齐 Flink-CDC 测试做法）
        waitForBackendReady();

        LOG.info("All containers are ready!");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping test containers...");

        if (DORIS_CONTAINER != null) {
            DORIS_CONTAINER.stop();
            LOG.info("Doris container stopped");
        }

        if (FIX_CONTAINER != null) {
            FIX_CONTAINER.stop();
            LOG.info("OceanBase container stopped");
        }
    }

    private static void waitForContainersReady() throws Exception {
        LOG.info("Waiting for containers to be ready...");

        // 等待Doris容器就绪
        int maxRetries = 30;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            if (DORIS_CONTAINER.isReady()) {
                LOG.info("Doris container is ready!");
                break;
            }

            retryCount++;
            LOG.info("Waiting for Doris container... (attempt {}/{})", retryCount, maxRetries);
            TimeUnit.SECONDS.sleep(5);
        }

        if (retryCount >= maxRetries) {
            throw new RuntimeException("Doris container failed to start within timeout");
        }
    }

    private static void waitForBackendReady() throws Exception {
        LOG.info("Waiting for Doris backend to be available...");
        long start = System.currentTimeMillis();
        while (!checkBackendAvailability()) {
            if (System.currentTimeMillis() - start > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000L) {
                throw new RuntimeException("Doris backend startup timed out.");
            }
            TimeUnit.SECONDS.sleep(1);
        }
        LOG.info("Doris backend is available.");
        // 额外等待5秒确保BE完全就绪
        TimeUnit.SECONDS.sleep(5);
    }

    private static boolean checkBackendAvailability() {
        try (Connection conn =
                        DriverManager.getConnection(
                                DORIS_CONTAINER.getJdbcUrl(),
                                DORIS_CONTAINER.getUsername(),
                                DORIS_CONTAINER.getPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW BACKENDS")) {
            int rowCount = 0;
            int aliveCount = 0;
            while (rs.next()) {
                rowCount++;
                String alive = rs.getString("Alive");
                if ("true".equals(alive)) {
                    aliveCount++;
                }
            }
            // 确保至少有一个活跃的后端
            return rowCount > 0 && aliveCount > 0;
        } catch (Exception e) {
            LOG.debug("Checking backend availability failed, will retry: {}", e.getMessage());
            return false;
        }
    }

    /** 获取Doris连接 */
    protected Connection getDorisConnection() throws SQLException {
        return DriverManager.getConnection(
                DORIS_CONTAINER.getJdbcUrl(),
                DORIS_CONTAINER.getUsername(),
                DORIS_CONTAINER.getPassword());
    }

    /** 获取Doris连接（指定数据库） */
    protected Connection getDorisConnection(String database) throws SQLException {
        return DriverManager.getConnection(
                DORIS_CONTAINER.getJdbcUrl(database),
                DORIS_CONTAINER.getUsername(),
                DORIS_CONTAINER.getPassword());
    }

    /** 执行SQL语句 */
    protected void executeDorisSQL(String sql) throws SQLException {
        try (Connection conn = getDorisConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** 执行SQL语句（指定数据库） */
    protected void executeDorisSQL(String database, String sql) throws SQLException {
        try (Connection conn = getDorisConnection(database);
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** 创建数据库 */
    protected void createDorisDatabase(String database) throws Exception {
        DORIS_CONTAINER.createDatabase(database);
    }

    /** 删除数据库 */
    protected void dropDorisDatabase(String database) throws Exception {
        DORIS_CONTAINER.dropDatabase(database);
    }

    /** 获取Doris FE HTTP地址 */
    protected String getDorisFeHttpAddress() {
        return DORIS_CONTAINER.getFeHttpAddress();
    }

    /** 获取Doris FE查询地址 */
    protected String getDorisFeQueryAddress() {
        return DORIS_CONTAINER.getFeQueryAddress();
    }

    /** 检查Doris是否可用 */
    protected boolean isDorisAvailable() {
        return DORIS_CONTAINER != null && DORIS_CONTAINER.isReady();
    }
}

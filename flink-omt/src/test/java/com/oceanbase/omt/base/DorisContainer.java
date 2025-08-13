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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Doris TestContainer implementation based on Flink CDC best practices.
 *
 * <p>This container provides a lightweight Doris FE instance for integration testing. It's designed
 * to be fast, reliable, and suitable for CI/CD environments.
 */
public class DorisContainer extends GenericContainer<DorisContainer> {

    // 使用 Doris all-in-one 镜像（FE+BE），适合集成测试（对齐 Flink-CDC 参考）
    private static final String DOCKER_IMAGE_NAME = "apache/doris:doris-all-in-one-2.1.0";

    // Doris FE端口
    public static final int FE_HTTP_PORT = 8030;
    public static final int FE_QUERY_PORT = 9030;
    public static final int FE_EDIT_LOG_PORT = 9010;
    // Doris BE 端口（对齐参考：8040）
    public static final int BE_HTTP_PORT = 8040;

    // 默认连接信息
    public static final String DEFAULT_USERNAME = "root";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_DATABASE = "test";

    public DorisContainer() {
        this(DOCKER_IMAGE_NAME);
    }

    public DorisContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName));
        configure();
    }

    public DorisContainer(Network network) {
        this();
        withNetwork(network);
        // Doris FE 依赖 FE_SERVERS 的别名解析，这里补充网络别名，保持与 FE_SERVERS 中的 fe1 一致
        withNetworkAliases("fe1");
    }

    protected void configure() {
        // 暴露必要端口（FE/BE）
        withExposedPorts(FE_HTTP_PORT, FE_QUERY_PORT, FE_EDIT_LOG_PORT, BE_HTTP_PORT);

        // all-in-one 镜像无需设置 FE_SERVERS/FE_ID

        // 等待策略：优先以健康检查接口作为就绪信号
        waitingFor(
                Wait.forHttp("/api/health")
                        .forPort(FE_HTTP_PORT)
                        .withStartupTimeout(Duration.ofMinutes(5)));

        // 使用镜像默认入口启动（不覆盖命令）
    }

    /** 获取FE HTTP端口 */
    public String getFeHttpAddress() {
        return getHost() + ":" + getMappedPort(FE_HTTP_PORT);
    }

    /** 获取FE查询端口 */
    public String getFeQueryAddress() {
        return getHost() + ":" + getMappedPort(FE_QUERY_PORT);
    }

    /** 获取 FeNodes（host:port） */
    public String getFeNodes() {
        return getHost() + ":" + getMappedPort(FE_HTTP_PORT);
    }

    /** 获取 BeNodes（host:port） */
    public String getBeNodes() {
        return getHost() + ":" + getMappedPort(BE_HTTP_PORT);
    }

    /** 获取JDBC URL */
    public String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%d", getHost(), getMappedPort(FE_QUERY_PORT));
    }

    /** 获取带数据库的JDBC URL */
    public String getJdbcUrl(String database) {
        return String.format(
                "jdbc:mysql://%s:%d/%s", getHost(), getMappedPort(FE_QUERY_PORT), database);
    }

    /** 获取用户名 */
    public String getUsername() {
        return DEFAULT_USERNAME;
    }

    /** 获取密码 */
    public String getPassword() {
        return DEFAULT_PASSWORD;
    }

    /** 获取驱动类名 */
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    /** 获取测试查询语句 */
    public String getTestQueryString() {
        return "SELECT 1";
    }

    /** 检查容器是否就绪 */
    public boolean isReady() {
        try {
            if (!isRunning()) {
                return false;
            }

            return isFrontendHealthy();
        } catch (Exception e) {
            return false;
        }
    }

    /** 检查 FE 健康状态 */
    public boolean isFrontendHealthy() {
        try {
            var result =
                    execInContainer(
                            "curl",
                            "-s",
                            "-o",
                            "/dev/null",
                            "-w",
                            "%{http_code}",
                            "http://localhost:" + FE_HTTP_PORT + "/api/health");
            return "200".equals(result.getStdout().trim());
        } catch (Exception e) {
            return false;
        }
    }

    /** 执行SQL语句 */
    public void executeSql(String sql) throws Exception {
        // 通过主机映射端口进行 JDBC 执行，避免依赖容器内是否预装 mysql 客户端
        try (Connection conn =
                        DriverManager.getConnection(
                                getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** 创建数据库 */
    public void createDatabase(String database) throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
        }
    }

    /** 删除数据库 */
    public void dropDatabase(String database) throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                getJdbcUrl(), getUsername(), getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + database);
        }
    }
}

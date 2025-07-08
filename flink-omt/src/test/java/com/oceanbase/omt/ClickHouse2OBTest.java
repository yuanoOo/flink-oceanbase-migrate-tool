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

import com.oceanbase.omt.base.OceanBaseMySQLTestBase;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.source.clickhouse.ClickHouseDatabaseSync;
import com.oceanbase.omt.utils.DataSourceUtils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ClickHouse2OBTest extends OceanBaseMySQLTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouse2OBTest.class);

    private static final String CLICKHOUSE_DOCKER_IMAGE_NAME =
            "clickhouse/clickhouse-server:latest";

    public static final GenericContainer<?> CLICKHOUSE_CONTAINER =
            new FixedHostPortGenericContainer<>(CLICKHOUSE_DOCKER_IMAGE_NAME)
                    .withNetwork(NETWORK)
                    .withFixedExposedPort(8123, 8123)
                    .withFixedExposedPort(9000, 9000)
                    .withEnv("CLICKHOUSE_PASSWORD", "123456")
                    .withEnv("CLICKHOUSE_USER", "root")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        FIX_CONTAINER.waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx(".*boot success!.*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(6)));
        FIX_CONTAINER.start();

        Startables.deepStart(Stream.of(CLICKHOUSE_CONTAINER)).join();
        try (ClickHouseContainer clickhouse =
                new ClickHouseContainer("clickhouse/clickhouse-server:latest")
                        .withPassword("123456")
                        .withUsername("root")
                        .withExposedPorts(8123, 9000)) {

            clickhouse.start();
            String username = clickhouse.getUsername();
            String password = clickhouse.getPassword();
            String jdbcUrl = "jdbc:clickhouse://localhost:8123/default?user=root&password=123456";

            try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT version()")) {
                if (rs.next()) {
                    System.out.println("ClickHouse version: " + rs.getString(1));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        CLICKHOUSE_CONTAINER.stop();
        FIX_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @Before
    public void init() throws IOException, SQLException {
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        crateDataBases(sourceDataSource.getConnection(), "test1");
        initialize(sourceDataSource.getConnection(), "sql/clickHouse-sql.sql");
    }

    @After
    public void close() throws IOException, SQLException {
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        // drop ob
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());
        dropDataBases(dataSource.getConnection(), "test1");
        // drop source
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        dropDataBases(sourceDataSource.getConnection(), "test1");
    }

    @Test
    public void testJdbc() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        ClickHouseDatabaseSync clickHouseDatabaseSync = new ClickHouseDatabaseSync(migrationConfig);
        clickHouseDatabaseSync.createTableInOb();
        clickHouseDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());
        // table1
        List<String> expected1 =
                Arrays.asList(
                        "1,2025-06-01 18:00:00,Alice,199.99,101,0",
                        "2,2025-06-01 18:05:00,Bob,299.99,102,0");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");
    }

    @Test
    public void testJdbcWithRoute() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse-route.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain StarRocks MetaData
        ClickHouseDatabaseSync clickHouseDatabaseSync = new ClickHouseDatabaseSync(migrationConfig);
        clickHouseDatabaseSync.createTableInOb();
        clickHouseDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

        // table1
        List<String> expected1 =
                Arrays.asList(
                        "1,2025-06-01 18:00:00,Alice,199.99,101,0",
                        "2,2025-06-01 18:05:00,Bob,299.99,102,0");
        assertContent(dataSource.getConnection(), expected1, "test1.order1");
    }

    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        assertEqualsInAnyOrder(expected, actual);
    }
}

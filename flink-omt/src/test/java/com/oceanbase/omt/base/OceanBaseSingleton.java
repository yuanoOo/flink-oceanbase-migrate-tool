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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.time.Duration;

public class OceanBaseSingleton {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLTestBase.class);
    private static final String CLUSTER_NAME = "spark-oceanbase-ci";
    private static final String SYS_PASSWORD = "123456";
    private static final String TEST_PASSWORD = "654321";

    public static final Network NETWORK = Network.newNetwork();

    public static final GenericContainer<?> FIX_CONTAINER =
            new FixedHostPortGenericContainer<>("oceanbase/oceanbase-ce:latest")
                    .withNetwork(NETWORK)
                    .withEnv("OB_TENANT_PASSWORD", TEST_PASSWORD)
                    .withEnv("MODE", "mini")
                    .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
                    .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                    .withEnv("OB_DATAFILE_SIZE", "2G")
                    .withEnv("OB_LOG_DISK_SIZE", "4G")
                    .withStartupTimeout(Duration.ofMinutes(6))
                    .withExposedPorts(2881, 2882)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    static {
        FIX_CONTAINER.waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx(".*boot success!.*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(6)));
        if (!FIX_CONTAINER.isRunning()) {
            FIX_CONTAINER.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(FIX_CONTAINER::stop));
    }

    public static GenericContainer<?> getInstance() {
        return FIX_CONTAINER;
    }
}

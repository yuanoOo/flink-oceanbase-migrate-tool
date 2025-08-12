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

package com.oceanbase.omt.directload.v2;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.omt.catalog.TableIdentifier;
import com.oceanbase.omt.directload.DirectLoadUtils;
import com.oceanbase.omt.directload.DirectLoader;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;

import java.io.IOException;
import java.util.Map;

public class GlobalCommiter implements Sink<CommittableMessage<String>> {
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final Map<TableIdentifier, String> tableIdExecutionIdMap;

    public GlobalCommiter(
            OBDirectLoadConnectorOptions connectorOptions,
            Map<TableIdentifier, String> tableIdExecutionIdMap) {
        this.connectorOptions = connectorOptions;
        this.tableIdExecutionIdMap = tableIdExecutionIdMap;
    }

    @Override
    public SinkWriter<CommittableMessage<String>> createWriter(InitContext context)
            throws IOException {
        return new GlobalCommiterWriter(connectorOptions, tableIdExecutionIdMap);
    }
}

class GlobalCommiterWriter implements SinkWriter<CommittableMessage<String>> {

    private final OBDirectLoadConnectorOptions connectorOptions;
    private final Map<TableIdentifier, String> tableIdExecutionIdMap;

    public GlobalCommiterWriter(
            OBDirectLoadConnectorOptions connectorOptions,
            Map<TableIdentifier, String> tableIdExecutionIdMap) {
        this.connectorOptions = connectorOptions;
        this.tableIdExecutionIdMap = tableIdExecutionIdMap;
    }

    @Override
    public void write(CommittableMessage<String> element, Context context)
            throws IOException, InterruptedException {}

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            tableIdExecutionIdMap.forEach(
                    (tableId, executionId) -> {
                        DirectLoader directLoader =
                                DirectLoadUtils.buildDirectLoaderFromConnOption(
                                        connectorOptions, tableId, executionId);
                        directLoader.begin();
                        directLoader.commit();
                    });
        }
    }

    @Override
    public void close() throws Exception {}
}

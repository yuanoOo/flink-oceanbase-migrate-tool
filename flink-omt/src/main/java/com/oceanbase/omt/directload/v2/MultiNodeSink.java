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
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.omt.catalog.TableIdentifier;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/** The direct-load sink. see {@link Sink}. */
public class MultiNodeSink<RowData>
        implements TwoPhaseCommittingSink<RowData, String>,
                WithPostCommitTopology<RowData, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiNodeSink.class);

    private final OBDirectLoadConnectorOptions connectorOptions;
    private final RecordSerializationSchema<RowData> recordSerializer;
    private final Map<TableIdentifier, String> tableIdExecutionIdMap;

    public MultiNodeSink(
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer,
            Map<TableIdentifier, String> tableIdExecutionIdMap) {
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.tableIdExecutionIdMap = tableIdExecutionIdMap;
    }

    @Override
    public PrecommittingSinkWriter<RowData, String> createWriter(InitContext context)
            throws IOException {
        return new MultiNodeWriter<>(connectorOptions, recordSerializer, tableIdExecutionIdMap);
    }

    @Override
    public Committer<String> createCommitter() throws IOException {
        return new NoOPCommiter(connectorOptions);
    }

    @Override
    public SimpleVersionedSerializer<String> getCommittableSerializer() {
        return new NoOpSerializer();
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<String>> commits) {
        commits.rebalance()
                .sinkTo(new GlobalCommiter(connectorOptions, tableIdExecutionIdMap))
                .name("Global Committer Node")
                .setParallelism(1);
    }
}

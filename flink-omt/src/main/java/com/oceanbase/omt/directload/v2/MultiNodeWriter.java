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
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.omt.catalog.TableIdentifier;
import com.oceanbase.omt.directload.DirectLoadUtils;
import com.oceanbase.omt.directload.DirectLoader;

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

public class MultiNodeWriter<T>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<T, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiNodeWriter.class);
    private final RecordSerializationSchema<T> recordSerializer;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final Map<TableIdentifier, DirectLoader> directLoadCache = new HashMap<>();
    private final List<DataChangeRecord> buffer;
    private final Map<TableIdentifier, String> tableIdExecutionIdMap;

    public MultiNodeWriter(
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<T> recordSerializer,
            Map<TableIdentifier, String> tableIdExecutionIdMap) {
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.buffer = Lists.newArrayListWithExpectedSize(connectorOptions.getBufferSize());
        this.tableIdExecutionIdMap = tableIdExecutionIdMap;
    }

    @Override
    public Collection<String> prepareCommit() throws IOException, InterruptedException {
        return Collections.emptyList();
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        DataChangeRecord record = (DataChangeRecord) recordSerializer.serialize(element);
        if (record == null) {
            return;
        }

        buffer.add(record);
        if (buffer.size() >= connectorOptions.getBufferSize()) {
            try {
                flush(buffer);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "The direct-load writer Failed to flash to table: %s.",
                                connectorOptions.getTableName()),
                        e);
            }
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            LOG.info("Flush buffer size: {}", buffer.size());
            try {
                flush(buffer);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "The direct-load writer Failed to flash to table: %s.",
                                connectorOptions.getTableName()),
                        e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (CollectionUtils.isNotEmpty(buffer)) {
            flush(buffer);
        }
        directLoadCache.forEach(
                (tableId, directLoader) -> {
                    directLoader.close();
                });
    }

    private void flush(List<DataChangeRecord> buffer) throws ObDirectLoadException {
        if (CollectionUtils.isEmpty(buffer)) {
            return;
        }
        Map<TableIdentifier, ObDirectLoadBucket> bucketMap = new HashMap<>();
        for (DataChangeRecord dataChangeRecord : buffer) {
            TableInfo table = (TableInfo) dataChangeRecord.getTable();
            TableIdentifier tableId =
                    new TableIdentifier(
                            table.getTableId().getSchemaName(), table.getTableId().getTableName());
            bucketMap.putIfAbsent(tableId, new ObDirectLoadBucket());
            List<String> fieldNames = table.getFieldNames();
            ObObj[] array = new ObObj[fieldNames.size()];
            int index = 0;
            for (String fieldName : fieldNames) {
                array[index++] = createObObj(dataChangeRecord.getFieldValue(fieldName));
            }
            ObDirectLoadBucket obDirectLoadBucket = bucketMap.get(tableId);
            obDirectLoadBucket.addRow(array);
        }
        bucketMap.forEach(
                (tableId, obDirectLoadBucket) -> {
                    try {
                        DirectLoader directLoader =
                                directLoadCache.computeIfAbsent(
                                        tableId,
                                        tableIdentifier -> {
                                            DirectLoader loader =
                                                    DirectLoadUtils.buildDirectLoaderFromConnOption(
                                                            connectorOptions,
                                                            tableId,
                                                            tableIdExecutionIdMap.get(tableId));
                                            loader.begin();
                                            LOG.info(
                                                    "The direct-load writer begin successfully with table {}.{}.",
                                                    tableId.getSchemaName(),
                                                    tableId.getTableName());
                                            return loader;
                                        });
                        directLoader.write(obDirectLoadBucket);
                    } catch (SQLException e) {
                        throw new RuntimeException(
                                String.format(
                                        "The direct-load writer Failed to flash to table: %s.",
                                        tableId.identifier()),
                                e);
                    }
                });
        buffer.clear();
    }
}

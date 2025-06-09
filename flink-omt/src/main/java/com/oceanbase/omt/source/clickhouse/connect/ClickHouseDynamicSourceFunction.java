package com.oceanbase.omt.source.clickhouse.connect;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;

public class ClickHouseDynamicSourceFunction extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData> {



    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}

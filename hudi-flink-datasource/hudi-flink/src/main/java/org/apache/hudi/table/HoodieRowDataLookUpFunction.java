package org.apache.hudi.table;

import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StringToRowDataConverter;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HoodieRowDataLookUpFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(HoodieRowDataLookUpFunction.class);
    private static final long serialVersionUID = 2L;

    private final HoodieFlinkTable table;
    private final DataType[] keyTypes;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final AvroToRowDataConverters jdbcRowConverter;
    private final StringToRowDataConverter stringToRowDataConverter;
    private final RowDataToAvroConverters rowDataToAvroConverters;

    private final Duration reloadInterval;
    private transient Map<RowData, List<RowData>> cache;

    public HoodieRowDataLookUpFunction(
            HoodieFlinkTable table,
            DataType[] keyTypes,
            String[] keyNames,
            long cacheMaxSize,
            long cacheExpireMs,
            int maxRetryTimes,
            AvroToRowDataConverters jdbcRowConverter,
            StringToRowDataConverter stringToRowDataConverter,
            RowDataToAvroConverters rowDataToAvroConverters, Duration reloadInterval) {
        this.table = table;
        this.keyTypes = keyTypes;
        this.keyNames = keyNames;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.jdbcRowConverter = jdbcRowConverter;
        this.stringToRowDataConverter = stringToRowDataConverter;
        this.rowDataToAvroConverters = rowDataToAvroConverters;
        this.reloadInterval = reloadInterval;
    }

    public void eval(Object... values) {
        RowData lookupKey = GenericRowData.of(values);
        List<RowData> matchedRows = cache.get(lookupKey);
        if (matchedRows != null) {
            for (RowData matchedRow : matchedRows) {
                collect(matchedRow);
            }
        }
    }
}

/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Wrapper for metrics-related operations.
 */
public class HoodieMetrics {
    private HoodieWriteConfig config = null;
    private String tableName = null;
    private static Logger logger = LogManager.getLogger(HoodieMetrics.class);
    // Some timers
    public String rollbackTimerName = null;
    public String cleanTimerName = null;
    public  String commitTimerName = null;
    private Timer rollbackTimer = null;
    private Timer cleanTimer = null;
    private Timer commitTimer = null;

    public HoodieMetrics(HoodieWriteConfig config, String tableName) {
        this.config = config;
        this.tableName = tableName;
        if (config.isMetricsOn()) {
            Metrics.init(config);
            this.rollbackTimerName = getMetricsName("timer", "rollback");
            this.cleanTimerName = getMetricsName("timer", "clean");
            this.commitTimerName = getMetricsName("timer", "commit");
        }
    }

    private Timer createTimer(String name) {
        return config.isMetricsOn() ? Metrics.getInstance().getRegistry().timer(name) : null;
    }

    public Timer.Context getRollbackCtx() {
        if (config.isMetricsOn() && rollbackTimer == null) {
            rollbackTimer = createTimer(rollbackTimerName);
        }
        return rollbackTimer == null ? null : rollbackTimer.time();
    }

    public Timer.Context getCleanCtx() {
        if (config.isMetricsOn() && cleanTimer == null) {
            cleanTimer = createTimer(cleanTimerName);
        }
        return cleanTimer == null ? null : cleanTimer.time();
    }

    public Timer.Context getCommitCtx() {
        if (config.isMetricsOn() && commitTimer == null) {
            commitTimer = createTimer(commitTimerName);
        }
        return commitTimer == null ? null : commitTimer.time();
    }

    public void updateCommitMetrics(long commitEpochTimeInMs, long durationInMs, HoodieCommitMetadata metadata) {
        if (config.isMetricsOn()) {
            long totalPartitionsWritten = metadata.fetchTotalPartitionsWritten();
            long totalFilesInsert = metadata.fetchTotalFilesInsert();
            long totalFilesUpdate = metadata.fetchTotalFilesUpdated();
            long totalRecordsWritten = metadata.fetchTotalRecordsWritten();
            long totalUpdateRecordsWritten = metadata.fetchTotalUpdateRecordsWritten();
            long totalInsertRecordsWritten = metadata.fetchTotalInsertRecordsWritten();
            long totalBytesWritten = metadata.fetchTotalBytesWritten();
            registerGauge(getMetricsName("commit", "duration"), durationInMs);
            registerGauge(getMetricsName("commit", "totalPartitionsWritten"), totalPartitionsWritten);
            registerGauge(getMetricsName("commit", "totalFilesInsert"), totalFilesInsert);
            registerGauge(getMetricsName("commit", "totalFilesUpdate"), totalFilesUpdate);
            registerGauge(getMetricsName("commit", "totalRecordsWritten"), totalRecordsWritten);
            registerGauge(getMetricsName("commit", "totalUpdateRecordsWritten"), totalUpdateRecordsWritten);
            registerGauge(getMetricsName("commit", "totalInsertRecordsWritten"), totalInsertRecordsWritten);
            registerGauge(getMetricsName("commit", "totalBytesWritten"), totalBytesWritten);
            registerGauge(getMetricsName("commit", "commitTime"), commitEpochTimeInMs);
        }
    }

    public void updateRollbackMetrics(long durationInMs, long numFilesDeleted) {
        if (config.isMetricsOn()) {
            logger.info(String.format("Sending rollback metrics (duration=%d, numFilesDeleted=$d)",
                    durationInMs, numFilesDeleted));
            registerGauge(getMetricsName("rollback", "duration"), durationInMs);
            registerGauge(getMetricsName("rollback", "numFilesDeleted"), numFilesDeleted);
        }
    }

    public void updateCleanMetrics(long durationInMs, int numFilesDeleted) {
        if (config.isMetricsOn()) {
            logger.info(String.format("Sending clean metrics (duration=%d, numFilesDeleted=%d)",
                    durationInMs, numFilesDeleted));
            registerGauge(getMetricsName("clean", "duration"), durationInMs);
            registerGauge(getMetricsName("clean", "numFilesDeleted"), numFilesDeleted);
        }
    }

    @VisibleForTesting
    String getMetricsName(String action, String metric) {
        return config == null ? null :
                String.format("%s.%s.%s", tableName, action, metric);
    }

    void registerGauge(String metricName, final long value) {
        try {
            MetricRegistry registry = Metrics.getInstance().getRegistry();
            registry.register(metricName, new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return value;
                }
            });
        } catch (Exception e) {
            // Here we catch all exception, so the major upsert pipeline will not be affected if the metrics system
            // has some issues.
            logger.error("Failed to send metrics: ", e);
        }
    }

    /**
     * By default, the timer context returns duration with nano seconds.
     * Convert it to millisecond.
     */
    public long getDurationInMs(long ctxDuration) {
        return ctxDuration / 1000000;
    }
}

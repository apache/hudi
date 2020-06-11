package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.junit.Test;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPrometheusReporter {

    HoodieWriteConfig config = mock(HoodieWriteConfig.class);

    @Test
    public void testRegisterGauge() {
        when(config.isMetricsOn()).thenReturn(true);
        when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS);
        when(config.getPrometheusHost()).thenReturn("localhost");
        when(config.getPrometheusPort()).thenReturn(9090);
        new HoodieMetrics(config, "raw_table");
        registerGauge("prometheusReporter_metric", 123L);
        assertEquals("123", Metrics.getInstance().getRegistry().getGauges()
                .get("prometheusReporter_metric_metric").getValue().toString());



    }
}
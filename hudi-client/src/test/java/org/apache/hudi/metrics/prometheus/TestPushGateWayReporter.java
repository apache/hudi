package org.apache.hudi.metrics.prometheus;

import com.codahale.metrics.MetricRegistry;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;
import org.junit.Test;
import org.mockito.Mock;

import static org.apache.hudi.metrics.Metrics.registerGauge;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPushGateWayReporter {

    HoodieWriteConfig config = mock(HoodieWriteConfig.class);

    @Test
    public void testRegisterGauge() {
        when(config.isMetricsOn()).thenReturn(true);
        when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS_PUSHGATEWAY);
        when(config.getPrometheusPushGatewayHost()).thenReturn("localhost");
        when(config.getPrometheusPushGatewayPort()).thenReturn(9091);
        new HoodieMetrics(config, "raw_table");
        registerGauge("pushGateWayReporter_metric", 123L);
        assertEquals("123", Metrics.getInstance().getRegistry().getGauges()
                .get("pushGateWayReporter_metric").getValue().toString());



    }
}
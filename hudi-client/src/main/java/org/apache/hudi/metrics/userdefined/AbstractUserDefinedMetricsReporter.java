package org.apache.hudi.metrics.userdefined;

import com.codahale.metrics.MetricRegistry;
import org.apache.hudi.metrics.MetricsReporter;
import java.util.Properties;

/**
 * Abstract class of user defined metrics reporter.
 */
public abstract class AbstractUserDefinedMetricsReporter extends MetricsReporter {
  private Properties props;
  private MetricRegistry registry;

  public AbstractUserDefinedMetricsReporter(Properties props, MetricRegistry registry) {
    this.props = props;
    this.registry = registry;
  }

  public Properties getProps() {
    return props;
  }

  public MetricRegistry getRegistry() {
    return registry;
  }
}
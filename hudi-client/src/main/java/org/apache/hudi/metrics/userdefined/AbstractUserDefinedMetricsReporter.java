package org.apache.hudi.metrics.userdefined;

import org.apache.hudi.metrics.MetricsReporter;
import java.util.Properties;

/**
 * Abstract class of user defined metrics reporter
 */
public abstract class AbstractUserDefinedMetricsReporter extends MetricsReporter {
    private Properties props;

    public AbstractUserDefinedMetricsReporter(Properties props) {
        this.props = props;
    }

    public Properties getProps() {
        return props;
    }
}
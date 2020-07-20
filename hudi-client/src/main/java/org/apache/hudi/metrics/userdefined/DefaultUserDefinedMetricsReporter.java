package org.apache.hudi.metrics.userdefined;

import java.io.Closeable;
import java.util.Properties;

public class DefaultUserDefinedMetricsReporter extends AbstractUserDefinedMetricsReporter {
    private Properties props;

    public DefaultUserDefinedMetricsReporter(Properties props) {
        super(props);
        this.props = props;
    }

    public Properties getProps() {
        return props;
    }

    @Override
    public void start() {

    }

    @Override
    public void report() {

    }

    @Override
    public Closeable getReporter() {
        return null;
    }

    @Override
    public void stop() {

    }
}
package org.apache.hudi.metrics.userdefined;

import java.io.Closeable;
import java.util.Properties;

/**
 * Used for testing.
 */
public class DefaultUserDefinedMetricsReporter extends AbstractUserDefinedMetricsReporter {

    public DefaultUserDefinedMetricsReporter(Properties props) {
        super(props);
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
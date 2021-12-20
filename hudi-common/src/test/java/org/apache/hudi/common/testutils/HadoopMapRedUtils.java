package org.apache.hudi.common.testutils;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hudi.common.util.Option;

import java.util.concurrent.ConcurrentHashMap;

public class HadoopMapRedUtils {

  /**
   * Creates instance of {@link Reporter} to collect reported counters
   */
  public static Reporter createTestReporter() {
    class TestReporter implements Reporter {
      private final ConcurrentHashMap<String, Counters.Counter> counters =
          new ConcurrentHashMap<>();

      @Override
      public void setStatus(String status) {
        // not-supported
      }

      @Override
      public Counters.Counter getCounter(Enum<?> name) {
        return counters.computeIfAbsent(name.name(), (ignored) -> new Counters.Counter());
      }

      @Override
      public Counters.Counter getCounter(String group, String name) {
        return counters.computeIfAbsent(getKey(group, name), (ignored) -> new Counters.Counter());
      }

      @Override
      public void incrCounter(Enum<?> key, long amount) {
        Option.ofNullable(counters.get(key))
            .ifPresent(c -> c.increment(amount));
      }

      @Override
      public void incrCounter(String group, String counter, long amount) {
        Option.ofNullable(counters.get(getKey(group, counter)))
            .ifPresent(c -> c.increment(amount));
      }

      @Override
      public InputSplit getInputSplit() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public float getProgress() {
        return -1;
      }

      @Override
      public void progress() {
        // not-supported
      }

      private String getKey(String group, String name) {
        return String.format("%s:%s", group, name);
      }
    }

    return new TestReporter();
  }
}

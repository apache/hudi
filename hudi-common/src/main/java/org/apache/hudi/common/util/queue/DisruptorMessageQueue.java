package org.apache.hudi.common.util.queue;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hudi.common.util.SizeEstimator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class DisruptorMessageQueue<I, O> extends HoodieMessageQueue<I, O> {

  private final Disruptor<HoodieDisruptorEvent<O>> queue;
  private final Function<I, O> transformFunction;
  private ExecutorService executorService;

  private final int bufferSize = 128 * 1024;

  public DisruptorMessageQueue(long bufferLimitInBytes, Function<I, O> transformFunction, SizeEstimator<O> sizeEstimator, int producerNumber) {
    this.executorService = Executors.newCachedThreadPool();

    if (producerNumber > 1) {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, executorService, ProducerType.MULTI, new BusySpinWaitStrategy());
    } else {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, executorService, ProducerType.SINGLE, new BusySpinWaitStrategy());
    }

    this.transformFunction = transformFunction;
  }

  @Override
  public long size() {
    return queue.getBufferSize();
  }

  @Override
  public void insertRecord(I value) throws Exception {
    O applied;
    if (value == null) {
      applied = null;
    } else {
      applied = transformFunction.apply(value);
    }

    EventTranslator<HoodieDisruptorEvent<O>> translator = new EventTranslator<HoodieDisruptorEvent<O>>() {
      @Override
      public void translateTo(HoodieDisruptorEvent<O> event, long sequence) {
        event.set(applied);
      }
    };

    queue.getRingBuffer().publishEvent(translator);
  }

  @Override
  public void close() {
    queue.shutdown();
  }

  public void shutdownNow() {
    executorService.shutdownNow();
  }

  public Disruptor<HoodieDisruptorEvent<O>> getInnerQueue() {
    return this.queue;
  }
}

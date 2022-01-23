/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase.trace;

import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Sampler;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This wrapper class provides functions for accessing htrace 4+ functionality in a simplified way.
 */
@InterfaceAudience.Private
public final class TraceUtil {
  private static HTraceConfiguration conf;
  private static Tracer tracer;

  private TraceUtil() {
  }

  /**
   * Wrapper method to create new TraceScope with the given description
   * @return TraceScope or null when not tracing
   */
  public static TraceScope createTrace(String description) {
    return (tracer == null) ? null : tracer.newScope(description);
  }

  /**
   * Wrapper method to create new child TraceScope with the given description
   * and parent scope's spanId
   * @param span parent span
   * @return TraceScope or null when not tracing
   */
  public static TraceScope createTrace(String description, Span span) {
    if (span == null) {
      return createTrace(description);
    }

    return (tracer == null) ? null : tracer.newScope(description, span.getSpanId());
  }

  /**
   * Wrapper method to add new sampler to the default tracer
   * @return true if added, false if it was already added
   */
  public static boolean addSampler(Sampler sampler) {
    if (sampler == null) {
      return false;
    }

    return (tracer == null) ? false : tracer.addSampler(sampler);
  }

  /**
   * Wrapper method to add key-value pair to TraceInfo of actual span
   */
  public static void addKVAnnotation(String key, String value){
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      span.addKVAnnotation(key, value);
    }
  }

  /**
   * Wrapper method to add receiver to actual tracerpool
   * @return true if successfull, false if it was already added
   */
  public static boolean addReceiver(SpanReceiver rcvr) {
    return (tracer == null) ? false : tracer.getTracerPool().addReceiver(rcvr);
  }

  /**
   * Wrapper method to remove receiver from actual tracerpool
   * @return true if removed, false if doesn't exist
   */
  public static boolean removeReceiver(SpanReceiver rcvr) {
    return (tracer == null) ? false : tracer.getTracerPool().removeReceiver(rcvr);
  }

  /**
   * Wrapper method to add timeline annotiation to current span with given message
   */
  public static void addTimelineAnnotation(String msg) {
    Span span = Tracer.getCurrentSpan();
    if (span != null) {
      span.addTimelineAnnotation(msg);
    }
  }

  /**
   * Wrap runnable with current tracer and description
   * @param runnable to wrap
   * @return wrapped runnable or original runnable when not tracing
   */
  public static Runnable wrap(Runnable runnable, String description) {
    return (tracer == null) ? runnable : tracer.wrap(runnable, description);
  }
}

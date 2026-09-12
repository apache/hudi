/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.util.Option;

import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/** Request for running one or more metadata table services. */
public final class MetadataTableServiceRequest implements Serializable {

  @Getter
  private final MetadataTableServiceMode mode;
  private final EnumSet<TableServiceType> services;
  @Getter
  private final Option<String> instantTime;
  private final boolean disableTableServiceManagerDelegation;

  private MetadataTableServiceRequest(Builder builder) {
    this.mode = builder.mode;
    this.services = builder.services.clone();
    this.instantTime = builder.instantTime;
    this.disableTableServiceManagerDelegation = builder.disableTableServiceManagerDelegation;
  }

  public Set<TableServiceType> getServices() {
    return Collections.unmodifiableSet(services);
  }

  public boolean includes(TableServiceType service) {
    return services.contains(service);
  }

  public boolean shouldDisableTableServiceManagerDelegation() {
    return disableTableServiceManagerDelegation;
  }

  /**
   * Returns a copy with the specified mode, preserving all other fields and validating the resulting request.
   * The original request is unchanged. An instant time is never discarded: a request containing one can
   * only be copied in {@link MetadataTableServiceMode#EXECUTE} mode.
   *
   * @throws IllegalArgumentException if the resulting request is invalid for the specified mode
   */
  public MetadataTableServiceRequest copy(MetadataTableServiceMode mode) {
    return newBuilder()
        .withMode(mode)
        .withServices(services)
        .withInstantTime(instantTime)
        .disableTableServiceManagerDelegation(disableTableServiceManagerDelegation)
        .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private MetadataTableServiceMode mode = MetadataTableServiceMode.SCHEDULE_AND_EXECUTE;
    private EnumSet<TableServiceType> services = EnumSet.of(
        TableServiceType.COMPACT, TableServiceType.LOG_COMPACT,
        TableServiceType.CLEAN, TableServiceType.ARCHIVE);
    private Option<String> instantTime = Option.empty();
    private boolean disableTableServiceManagerDelegation;

    public Builder withMode(MetadataTableServiceMode mode) {
      this.mode = mode;
      return this;
    }

    public Builder withServices(Set<TableServiceType> services) {
      this.services = services.isEmpty()
          ? EnumSet.noneOf(TableServiceType.class)
          : EnumSet.copyOf(services);
      return this;
    }

    /**
     * Sets an optional instant to execute. A present instant requires {@link MetadataTableServiceMode#EXECUTE}
     * mode and exactly one service: compaction or log compaction. These constraints are checked by {@link #build()}.
     */
    public Builder withInstantTime(Option<String> instantTime) {
      this.instantTime = instantTime;
      return this;
    }

    public Builder disableTableServiceManagerDelegation(boolean disableDelegation) {
      this.disableTableServiceManagerDelegation = disableDelegation;
      return this;
    }

    public MetadataTableServiceRequest build() {
      if (mode == null) {
        throw new IllegalArgumentException("Metadata table service mode must be specified");
      }
      if (services.isEmpty()) {
        throw new IllegalArgumentException("At least one metadata table service must be specified");
      }
      if (services.contains(TableServiceType.CLUSTER)) {
        throw new IllegalArgumentException("Clustering is not supported as a metadata table service");
      }
      if (instantTime.isPresent()) {
        long executableCompactionServices = services.stream()
            .filter(service -> service == TableServiceType.COMPACT || service == TableServiceType.LOG_COMPACT)
            .count();
        if (mode != MetadataTableServiceMode.EXECUTE
            || executableCompactionServices != 1
            || services.size() != 1) {
          throw new IllegalArgumentException(
              "An instant time requires execute mode and exactly one of compaction or log compaction");
        }
      }
      return new MetadataTableServiceRequest(this);
    }
  }
}

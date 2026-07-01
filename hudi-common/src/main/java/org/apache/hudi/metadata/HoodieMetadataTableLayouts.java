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

package org.apache.hudi.metadata;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieMetadataException;

/**
 * Factory for resolving the {@link HoodieMetadataTableLayout} from an MDT's
 * persisted {@link HoodieTableConfig}.
 *
 * <p>Resolution rules:
 * <ol>
 *   <li>If {@link HoodieTableConfig#METADATA_LAYOUT_CLASS} is unset, return
 *   {@link FlatMDTLayout}. This is the case for every existing MDT and
 *   preserves backward compatibility.</li>
 *   <li>If set to the FQCN of {@link FlatMDTLayout} or
 *   {@link SubDirBucketedMDTLayout}, the corresponding built-in instance is
 *   returned. {@code SubDirBucketedMDTLayout} reads its bucket size from
 *   {@link HoodieTableConfig#METADATA_LAYOUT_BUCKET_SIZE}.</li>
 *   <li>Otherwise, the class is loaded reflectively. The class must either
 *   have a no-arg constructor or a constructor that accepts a single
 *   {@link HoodieTableConfig}.</li>
 * </ol>
 */
public final class HoodieMetadataTableLayouts {

  private HoodieMetadataTableLayouts() {
  }

  /**
   * Resolve the layout for the given MDT table config.
   *
   * @param mdtConfig the {@link HoodieTableConfig} of the MDT (not the data table)
   */
  public static HoodieMetadataTableLayout load(HoodieTableConfig mdtConfig) {
    Option<String> layoutClass = mdtConfig.getMetadataLayoutClass();
    if (!layoutClass.isPresent() || layoutClass.get().isEmpty()) {
      return new FlatMDTLayout();
    }
    String cls = layoutClass.get();
    if (FlatMDTLayout.class.getName().equals(cls)) {
      return new FlatMDTLayout();
    }
    if (SubDirBucketedMDTLayout.class.getName().equals(cls)) {
      return new SubDirBucketedMDTLayout(mdtConfig.getMetadataLayoutBucketSize());
    }
    try {
      Object instance;
      if (ReflectionUtils.hasConstructor(cls, new Class<?>[] {HoodieTableConfig.class}, true)) {
        instance = ReflectionUtils.loadClass(cls, new Class<?>[] {HoodieTableConfig.class}, mdtConfig);
      } else {
        instance = ReflectionUtils.loadClass(cls);
      }
      if (!(instance instanceof HoodieMetadataTableLayout)) {
        throw new HoodieMetadataException("Configured MDT layout class " + cls
            + " is not a HoodieMetadataTableLayout");
      }
      return (HoodieMetadataTableLayout) instance;
    } catch (Exception e) {
      throw new HoodieMetadataException("Failed to load MDT layout class " + cls
          + ". The MDT was initialized with this layout class; the same class must be available "
          + "on the classpath of any writer or reader opening the table.", e);
    }
  }
}

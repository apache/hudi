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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSourceContracts {

  /** A source class whose static initializer fails: resolving its contract must not initialize it. */
  public abstract static class UninitializableSource extends JsonDFSSource {
    static final int VALUE = fail();

    private UninitializableSource() {
      super(null, null, null, null);
    }

    private static int fail() {
      throw new IllegalStateException("initialized");
    }
  }

  /** A subclass of a registered source. */
  public abstract static class UninitializableSubclass extends UninitializableSource {
    private UninitializableSubclass() {
      super();
    }
  }

  /** A contract that marks the properties it prepares. */
  static final class MarkingContract implements SourceContract {
    private final TypedProperties seen;

    MarkingContract(TypedProperties seen) {
      this.seen = seen;
    }

    @Override
    public TypedProperties prepareJob(TypedProperties props) {
      TypedProperties prepared = TypedProperties.copy(props);
      prepared.setProperty("hoodie.test.prepared", "true");
      return prepared;
    }
  }

  @Test
  void everySourceHasNoContractWhileNoneIsRegistered() {
    TypedProperties props = new TypedProperties();
    for (String source : new String[] {null, "", " ", "missing.Source", JsonDFSSource.class.getName(),
        UninitializableSource.class.getName()}) {
      assertSame(SourceContracts.NONE, SourceContracts.of(source, props), String.valueOf(source));
    }
  }

  @Test
  void noContractKeepsTheJobAsConfigured() {
    SourceContract contract = SourceContracts.NONE;
    TypedProperties props = new TypedProperties();
    props.setProperty("k", "v");
    List<String> chain = Collections.singletonList("user.Transformer");
    assertSame(props, contract.prepareJob(props));
    assertFalse(contract.isPreparedJob(props));
    assertSame(chain, contract.transformerChain(chain));
    assertNull(contract.transformerChain(null));
    assertTrue(contract.tableProperties().isEmpty());
    contract.checkTable("/tmp/table", null);
    contract.checkJob(null, null, chain, props, Option.empty(), Option.empty());
    assertEquals("v", props.getString("k"));
  }

  @Test
  void registeredSourceAndSubclassResolveWithoutInitialization() {
    Map<Class<?>, Function<TypedProperties, SourceContract>> factories =
        Collections.singletonMap(UninitializableSource.class, MarkingContract::new);
    TypedProperties props = new TypedProperties();
    for (Class<?> source : new Class<?>[] {UninitializableSource.class, UninitializableSubclass.class}) {
      SourceContract contract = SourceContracts.of(" " + source.getName() + " ", props, factories);
      assertTrue(contract instanceof MarkingContract, source.getName());
      assertEquals("true", contract.prepareJob(props).getString("hoodie.test.prepared"));
      assertFalse(props.containsKey("hoodie.test.prepared"), "the caller's properties are not changed");
    }
    assertSame(SourceContracts.NONE, SourceContracts.of(JsonDFSSource.class.getName(), props, factories),
        "a registered subclass does not cover its superclass");
    assertSame(SourceContracts.NONE, SourceContracts.of("missing.Source", props, factories));
  }

  @Test
  void factoryRefusingTheJobPropagates() {
    Map<Class<?>, Function<TypedProperties, SourceContract>> factories = Collections.singletonMap(JsonDFSSource.class, props -> {
      throw new IllegalArgumentException("refused");
    });
    assertThrows(IllegalArgumentException.class, () -> SourceContracts.of(JsonDFSSource.class.getName(), new TypedProperties(), factories));
  }
}

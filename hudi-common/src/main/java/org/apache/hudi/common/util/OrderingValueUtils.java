package org.apache.hudi.common.util;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.math.BigDecimal;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * “License”); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Utils for orderingValue process.
 */
public class OrderingValueUtils {
    public static Pair<Comparable, Comparable> canonicalizeOrderingValue(Comparable oldOrder, Comparable incomingOrder) {
        if (oldOrder instanceof Utf8 && incomingOrder instanceof String) {
            oldOrder = oldOrder.toString();
        }
        if (incomingOrder instanceof Utf8 && oldOrder instanceof String) {
            incomingOrder = incomingOrder.toString();
        }
        if (oldOrder instanceof GenericData.Fixed && incomingOrder instanceof BigDecimal) {
            oldOrder = (BigDecimal) HoodieAvroUtils.convertValueForSpecificDataTypes(((GenericData.Fixed) oldOrder).getSchema(), oldOrder, false);
        }
        if (incomingOrder instanceof GenericData.Fixed && oldOrder instanceof BigDecimal) {
            incomingOrder = (BigDecimal) HoodieAvroUtils.convertValueForSpecificDataTypes(((GenericData.Fixed) incomingOrder).getSchema(), incomingOrder, false);
        }

        return new ImmutablePair<>(oldOrder, incomingOrder);
    }
}

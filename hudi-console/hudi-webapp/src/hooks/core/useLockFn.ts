/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { ref, unref } from 'vue';

export function useLockFn<P extends any[] = any[], V = any>(fn: (...args: P) => Promise<V>) {
  const lockRef = ref(false);
  return async function (...args: P) {
    if (unref(lockRef)) return;
    lockRef.value = true;
    try {
      const ret = await fn(...args);
      lockRef.value = false;
      return ret;
    } catch (e) {
      lockRef.value = false;
      throw e;
    }
  };
}

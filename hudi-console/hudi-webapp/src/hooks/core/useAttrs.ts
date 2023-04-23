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
import { getCurrentInstance, reactive, shallowRef, watchEffect } from 'vue';
import type { Ref } from 'vue';
interface Params {
  excludeListeners?: boolean;
  excludeKeys?: string[];
  excludeDefaultKeys?: boolean;
}

const DEFAULT_EXCLUDE_KEYS = ['class', 'style'];
const LISTENER_PREFIX = /^on[A-Z]/;

export function entries<T>(obj: Recordable<T>): [string, T][] {
  return Object.keys(obj).map((key: string) => [key, obj[key]]);
}

export function useAttrs(params: Params = {}): Ref<Recordable> | {} {
  const instance = getCurrentInstance();
  if (!instance) return {};

  const { excludeListeners = false, excludeKeys = [], excludeDefaultKeys = true } = params;
  const attrs = shallowRef({});
  const allExcludeKeys = excludeKeys.concat(excludeDefaultKeys ? DEFAULT_EXCLUDE_KEYS : []);

  // Since attrs are not reactive, make it reactive instead of doing in `onUpdated` hook for better performance
  instance.attrs = reactive(instance.attrs);

  watchEffect(() => {
    const res = entries(instance.attrs).reduce((acm, [key, val]) => {
      if (!allExcludeKeys.includes(key) && !(excludeListeners && LISTENER_PREFIX.test(key))) {
        acm[key] = val;
      }

      return acm;
    }, {} as Recordable);

    attrs.value = res;
  });

  return attrs;
}

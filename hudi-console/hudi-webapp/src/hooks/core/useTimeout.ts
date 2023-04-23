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
import { ref, watch } from 'vue';
import { tryOnUnmounted } from '@vueuse/core';
import { isFunction } from '/@/utils/is';

export function useTimeoutFn(handle: Fn<any>, wait: number, native = false) {
  if (!isFunction(handle)) {
    throw new Error('handle is not Function!');
  }

  const { readyRef, stop, start } = useTimeoutRef(wait);
  if (native) {
    handle();
  } else {
    watch(
      readyRef,
      (maturity) => {
        maturity && handle();
      },
      { immediate: false },
    );
  }
  return { readyRef, stop, start };
}

export function useTimeoutRef(wait: number) {
  const readyRef = ref(false);

  let timer: TimeoutHandle;
  function stop(): void {
    readyRef.value = false;
    timer && window.clearTimeout(timer);
  }
  function start(): void {
    stop();
    timer = setTimeout(() => {
      readyRef.value = true;
    }, wait);
  }

  start();

  tryOnUnmounted(stop);

  return { readyRef, stop, start };
}

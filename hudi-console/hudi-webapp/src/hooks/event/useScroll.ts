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
import type { Ref } from 'vue';

import { ref, onMounted, watch, onUnmounted } from 'vue';
import { isWindow, isObject } from '/@/utils/is';
import { useThrottleFn } from '@vueuse/core';

export function useScroll(
  refEl: Ref<Element | Window | null>,
  options?: {
    wait?: number;
    leading?: boolean;
    trailing?: boolean;
  },
) {
  const refX = ref(0);
  const refY = ref(0);
  let handler = () => {
    if (isWindow(refEl.value)) {
      refX.value = refEl.value.scrollX;
      refY.value = refEl.value.scrollY;
    } else if (refEl.value) {
      refX.value = (refEl.value as Element).scrollLeft;
      refY.value = (refEl.value as Element).scrollTop;
    }
  };

  if (isObject(options)) {
    let wait = 0;
    if (options.wait && options.wait > 0) {
      wait = options.wait;
      Reflect.deleteProperty(options, 'wait');
    }

    handler = useThrottleFn(handler, wait);
  }

  let stopWatch: () => void;
  onMounted(() => {
    stopWatch = watch(
      refEl,
      (el, prevEl, onCleanup) => {
        if (el) {
          el.addEventListener('scroll', handler);
        } else if (prevEl) {
          prevEl.removeEventListener('scroll', handler);
        }
        onCleanup(() => {
          refX.value = refY.value = 0;
          el && el.removeEventListener('scroll', handler);
        });
      },
      { immediate: true },
    );
  });

  onUnmounted(() => {
    refEl.value && refEl.value.removeEventListener('scroll', handler);
  });

  function stop() {
    stopWatch && stopWatch();
  }

  return { refX, refY, stop };
}

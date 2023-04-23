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
import { ref, watch, unref } from 'vue';
import { useThrottleFn, useDebounceFn } from '@vueuse/core';

export type RemoveEventFn = () => void;
export interface UseEventParams {
  el?: Element | Ref<Element | undefined> | Window | any;
  name: string;
  listener: EventListener;
  options?: boolean | AddEventListenerOptions;
  autoRemove?: boolean;
  isDebounce?: boolean;
  wait?: number;
}
export function useEventListener({
  el = window,
  name,
  listener,
  options,
  autoRemove = true,
  isDebounce = true,
  wait = 80,
}: UseEventParams): { removeEvent: RemoveEventFn } {
  /* eslint-disable-next-line */
  let remove: RemoveEventFn = () => {};
  const isAddRef = ref(false);

  if (el) {
    const element = ref(el as Element) as Ref<Element>;

    const handler = isDebounce ? useDebounceFn(listener, wait) : useThrottleFn(listener, wait);
    const realHandler = wait ? handler : listener;
    const removeEventListener = (e: Element) => {
      isAddRef.value = true;
      e.removeEventListener(name, realHandler, options);
    };
    const addEventListener = (e: Element) => e.addEventListener(name, realHandler, options);

    const removeWatch = watch(
      element,
      (v, _ov, cleanUp) => {
        if (v) {
          !unref(isAddRef) && addEventListener(v);
          cleanUp(() => {
            autoRemove && removeEventListener(v);
          });
        }
      },
      { immediate: true },
    );

    remove = () => {
      removeEventListener(element.value);
      removeWatch();
    };
  }
  return { removeEvent: remove };
}

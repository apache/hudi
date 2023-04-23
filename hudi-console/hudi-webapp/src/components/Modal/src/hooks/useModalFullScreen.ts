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
import { computed, Ref, ref, unref } from 'vue';

export interface UseFullScreenContext {
  wrapClassName: Ref<string | undefined>;
  modalWrapperRef: Ref<ComponentRef>;
  extHeightRef: Ref<number>;
}

export function useFullScreen(context: UseFullScreenContext) {
  // const formerHeightRef = ref(0);
  const fullScreenRef = ref(false);

  const getWrapClassName = computed(() => {
    const clsName = unref(context.wrapClassName) || '';
    return unref(fullScreenRef) ? `fullscreen-modal ${clsName} ` : unref(clsName);
  });

  function handleFullScreen(e: Event) {
    e && e.stopPropagation();
    fullScreenRef.value = !unref(fullScreenRef);

    // const modalWrapper = unref(context.modalWrapperRef);

    // if (!modalWrapper) return;

    // const wrapperEl = modalWrapper.$el as HTMLElement;
    // if (!wrapperEl) return;
    // const modalWrapSpinEl = wrapperEl.querySelector('.ant-spin-nested-loading') as HTMLElement;

    // if (!modalWrapSpinEl) return;

    // if (!unref(formerHeightRef) && unref(fullScreenRef)) {
    //   formerHeightRef.value = modalWrapSpinEl.offsetHeight;
    // }

    // if (unref(fullScreenRef)) {
    //   modalWrapSpinEl.style.height = `${window.innerHeight - unref(context.extHeightRef)}px`;
    // } else {
    //   modalWrapSpinEl.style.height = `${unref(formerHeightRef)}px`;
    // }
  }
  return { getWrapClassName, handleFullScreen, fullScreenRef };
}

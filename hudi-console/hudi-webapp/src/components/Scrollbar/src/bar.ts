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
import {
  defineComponent,
  h,
  computed,
  ref,
  getCurrentInstance,
  onUnmounted,
  inject,
  Ref,
} from 'vue';
import { on, off } from '/@/utils/domUtils';

import { renderThumbStyle, BAR_MAP } from './util';

export default defineComponent({
  name: 'Bar',

  props: {
    vertical: Boolean,
    size: String,
    move: Number,
  },

  setup(props) {
    const instance = getCurrentInstance();
    const thumb = ref();
    const wrap = inject('scroll-bar-wrap', {} as Ref<Nullable<HTMLElement>>) as any;
    const bar = computed(() => {
      return BAR_MAP[props.vertical ? 'vertical' : 'horizontal'];
    });
    const barStore = ref<Recordable>({});
    const cursorDown = ref();
    const clickThumbHandler = (e: any) => {
      // prevent click event of right button
      if (e.ctrlKey || e.button === 2) {
        return;
      }
      window.getSelection()?.removeAllRanges();
      startDrag(e);
      barStore.value[bar.value.axis] =
        e.currentTarget[bar.value.offset] -
        (e[bar.value.client] - e.currentTarget.getBoundingClientRect()[bar.value.direction]);
    };

    const clickTrackHandler = (e: any) => {
      const offset = Math.abs(
        e.target.getBoundingClientRect()[bar.value.direction] - e[bar.value.client],
      );
      const thumbHalf = thumb.value[bar.value.offset] / 2;
      const thumbPositionPercentage =
        ((offset - thumbHalf) * 100) / instance?.vnode.el?.[bar.value.offset];

      wrap.value[bar.value.scroll] =
        (thumbPositionPercentage * wrap.value[bar.value.scrollSize]) / 100;
    };
    const startDrag = (e: any) => {
      e.stopImmediatePropagation();
      cursorDown.value = true;
      on(document, 'mousemove', mouseMoveDocumentHandler);
      on(document, 'mouseup', mouseUpDocumentHandler);
      document.onselectstart = () => false;
    };

    const mouseMoveDocumentHandler = (e: any) => {
      if (cursorDown.value === false) return;
      const prevPage = barStore.value[bar.value.axis];

      if (!prevPage) return;

      const offset =
        (instance?.vnode.el?.getBoundingClientRect()[bar.value.direction] - e[bar.value.client]) *
        -1;
      const thumbClickPosition = thumb.value[bar.value.offset] - prevPage;
      const thumbPositionPercentage =
        ((offset - thumbClickPosition) * 100) / instance?.vnode.el?.[bar.value.offset];
      wrap.value[bar.value.scroll] =
        (thumbPositionPercentage * wrap.value[bar.value.scrollSize]) / 100;
    };

    function mouseUpDocumentHandler() {
      cursorDown.value = false;
      barStore.value[bar.value.axis] = 0;
      off(document, 'mousemove', mouseMoveDocumentHandler);
      document.onselectstart = null;
    }

    onUnmounted(() => {
      off(document, 'mouseup', mouseUpDocumentHandler);
    });

    return () =>
      h(
        'div',
        {
          class: ['scrollbar__bar', 'is-' + bar.value.key],
          onMousedown: clickTrackHandler,
        },
        h('div', {
          ref: thumb,
          class: 'scrollbar__thumb',
          onMousedown: clickThumbHandler,
          style: renderThumbStyle({
            size: props.size,
            move: props.move,
            bar: bar.value,
          }),
        }),
      );
  },
});

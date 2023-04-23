<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<template>
  <Scrollbar ref="scrollbarRef" class="scroll-container" v-bind="$attrs">
    <slot></slot>
  </Scrollbar>
</template>

<script lang="ts">
  import { defineComponent, ref, unref, nextTick } from 'vue';
  import { Scrollbar, ScrollbarType } from '/@/components/Scrollbar';
  import { useScrollTo } from '/@/hooks/event/useScrollTo';

  export default defineComponent({
    name: 'ScrollContainer',
    components: { Scrollbar },
    setup() {
      const scrollbarRef = ref<Nullable<ScrollbarType>>(null);

      /**
       * Scroll to the specified position
       */
      function scrollTo(to: number, duration = 500) {
        const scrollbar = unref(scrollbarRef);
        if (!scrollbar) {
          return;
        }
        nextTick(() => {
          const wrap = unref(scrollbar.wrap);
          if (!wrap) {
            return;
          }
          const { start } = useScrollTo({
            el: wrap,
            to,
            duration,
          });
          start();
        });
      }

      function getScrollWrap() {
        const scrollbar = unref(scrollbarRef);
        if (!scrollbar) {
          return null;
        }
        return scrollbar.wrap;
      }

      /**
       * Scroll to the bottom
       */
      function scrollBottom() {
        const scrollbar = unref(scrollbarRef);
        if (!scrollbar) {
          return;
        }
        nextTick(() => {
          const wrap = unref(scrollbar.wrap) as any;
          if (!wrap) {
            return;
          }
          const scrollHeight = wrap.scrollHeight as number;
          const { start } = useScrollTo({
            el: wrap,
            to: scrollHeight,
          });
          start();
        });
      }

      return {
        scrollbarRef,
        scrollTo,
        scrollBottom,
        getScrollWrap,
      };
    },
  });
</script>
<style lang="less">
  .scroll-container {
    width: 100%;
    height: 100%;

    .scrollbar__wrap {
      margin-bottom: 18px !important;
      overflow-x: hidden;
    }

    .scrollbar__view {
      box-sizing: border-box;
    }
  }
</style>

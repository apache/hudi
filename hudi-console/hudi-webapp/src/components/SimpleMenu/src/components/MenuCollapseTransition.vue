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
  <transition mode="out-in" v-on="on">
    <slot></slot>
  </transition>
</template>
<script lang="ts">
  import { defineComponent } from 'vue';
  import { addClass, removeClass } from '/@/utils/domUtils';

  export default defineComponent({
    name: 'MenuCollapseTransition',
    setup() {
      return {
        on: {
          beforeEnter(el) {
            addClass(el, 'collapse-transition');
            if (!el.dataset) el.dataset = {};

            el.dataset.oldPaddingTop = el.style.paddingTop;
            el.dataset.oldPaddingBottom = el.style.paddingBottom;

            el.style.height = '0';
            el.style.paddingTop = 0;
            el.style.paddingBottom = 0;
          },

          enter(el) {
            el.dataset.oldOverflow = el.style.overflow;
            if (el.scrollHeight !== 0) {
              el.style.height = el.scrollHeight + 'px';
              el.style.paddingTop = el.dataset.oldPaddingTop;
              el.style.paddingBottom = el.dataset.oldPaddingBottom;
            } else {
              el.style.height = '';
              el.style.paddingTop = el.dataset.oldPaddingTop;
              el.style.paddingBottom = el.dataset.oldPaddingBottom;
            }

            el.style.overflow = 'hidden';
          },

          afterEnter(el) {
            removeClass(el, 'collapse-transition');
            el.style.height = '';
            el.style.overflow = el.dataset.oldOverflow;
          },

          beforeLeave(el) {
            if (!el.dataset) el.dataset = {};
            el.dataset.oldPaddingTop = el.style.paddingTop;
            el.dataset.oldPaddingBottom = el.style.paddingBottom;
            el.dataset.oldOverflow = el.style.overflow;

            el.style.height = el.scrollHeight + 'px';
            el.style.overflow = 'hidden';
          },

          leave(el) {
            if (el.scrollHeight !== 0) {
              addClass(el, 'collapse-transition');
              el.style.height = 0;
              el.style.paddingTop = 0;
              el.style.paddingBottom = 0;
            }
          },

          afterLeave(el) {
            removeClass(el, 'collapse-transition');
            el.style.height = '';
            el.style.overflow = el.dataset.oldOverflow;
            el.style.paddingTop = el.dataset.oldPaddingTop;
            el.style.paddingBottom = el.dataset.oldPaddingBottom;
          },
        },
      };
    },
  });
</script>

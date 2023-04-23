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
  <section
    class="full-loading"
    :class="{ absolute, [theme]: !!theme }"
    :style="[background ? `background-color: ${background}` : '']"
    v-show="loading"
  >
    <Spin v-bind="$attrs" :tip="tip" :size="size" :spinning="loading" />
  </section>
</template>
<script lang="ts">
  import { PropType } from 'vue';
  import { defineComponent } from 'vue';
  import { Spin } from 'ant-design-vue';
  import { SizeEnum } from '/@/enums/sizeEnum';

  export default defineComponent({
    name: 'Loading',
    components: { Spin },
    props: {
      tip: {
        type: String as PropType<string>,
        default: '',
      },
      size: {
        type: String as PropType<SizeEnum>,
        default: SizeEnum.LARGE,
        validator: (v: SizeEnum): boolean => {
          return [SizeEnum.DEFAULT, SizeEnum.SMALL, SizeEnum.LARGE].includes(v);
        },
      },
      absolute: {
        type: Boolean as PropType<boolean>,
        default: false,
      },
      loading: {
        type: Boolean as PropType<boolean>,
        default: false,
      },
      background: {
        type: String as PropType<string>,
      },
      theme: {
        type: String as PropType<'dark' | 'light'>,
        default: 'light',
      },
    },
  });
</script>
<style lang="less" scoped>
  .full-loading {
    position: fixed;
    top: 0;
    left: 0;
    z-index: 200;
    display: flex;
    width: 100%;
    height: 100%;
    justify-content: center;
    align-items: center;
    background-color: rgb(240 242 245 / 40%);

    &.absolute {
      position: absolute;
      top: 0;
      left: 0;
      z-index: 300;
    }
  }

  html[data-theme='dark'] {
    .full-loading:not(.light) {
      background-color: @modal-mask-bg;
    }
  }

  .full-loading.dark {
    background-color: @modal-mask-bg;
  }
</style>

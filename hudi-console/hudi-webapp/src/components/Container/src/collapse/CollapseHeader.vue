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
  <div :class="[`${prefixCls}__header px-2 py-5`, $attrs.class]">
    <BasicTitle :helpMessage="helpMessage" normal>
      <template v-if="title">
        {{ title }}
      </template>
      <template v-else>
        <slot name="title"></slot>
      </template>
    </BasicTitle>
    <div :class="`${prefixCls}__action`">
      <slot name="action"></slot>
      <BasicArrow v-if="canExpan" up :expand="show" @click="$emit('expand')" />
    </div>
  </div>
</template>
<script lang="ts">
  import { defineComponent } from 'vue';
  import { BasicArrow, BasicTitle } from '/@/components/Basic';

  const props = {
    prefixCls: { type: String },
    helpMessage: {
      type: [Array, String] as PropType<string[] | string>,
      default: '',
    },
    title: { type: String },
    show: { type: Boolean },
    canExpan: { type: Boolean },
  };

  export default defineComponent({
    components: { BasicArrow, BasicTitle },
    inheritAttrs: false,
    props,
    emits: ['expand'],
  });
</script>

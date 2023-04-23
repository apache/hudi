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
  <Button v-bind="getBindValue" :class="getButtonClass" @click="onClick">
    <template #default="data">
      <Icon :icon="preIcon" v-if="preIcon" :size="iconSize" />
      <slot v-bind="data || {}"></slot>
      <Icon :icon="postIcon" v-if="postIcon" :size="iconSize" />
    </template>
  </Button>
</template>

<script lang="ts">
  import { defineComponent } from 'vue';
  export default defineComponent({
    name: 'AButton',
    inheritAttrs: false,
  });
</script>
<script lang="ts" setup>
  import { computed, unref } from 'vue';
  import { Button } from 'ant-design-vue';
  import Icon from '/@/components/Icon/src/Icon.vue';
  import { buttonProps } from './props';
  import { useAttrs } from '/@/hooks/core/useAttrs';

  const props = defineProps(buttonProps);
  // get component class
  const attrs = useAttrs({ excludeDefaultKeys: false });
  const getButtonClass = computed(() => {
    const { color, disabled } = props;
    return [
      {
        [`ant-btn-${color}`]: !!color,
        [`is-disabled`]: disabled,
      },
    ];
  });

  // get inherit binding value
  const getBindValue = computed(() => ({ ...unref(attrs), ...props }));
</script>

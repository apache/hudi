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
  <EditTableHeaderCell v-if="getIsEdit">
    {{ getTitle }}
  </EditTableHeaderCell>
  <span v-else>{{ getTitle }}</span>
  <BasicHelp v-if="getHelpMessage" :text="getHelpMessage" :class="`${prefixCls}__help`" />
</template>
<script lang="ts">
  import type { PropType } from 'vue';
  import type { BasicColumn } from '../types/table';
  import { defineComponent, computed } from 'vue';
  import BasicHelp from '/@/components/Basic/src/BasicHelp.vue';
  import EditTableHeaderCell from './EditTableHeaderIcon.vue';
  import { useDesign } from '/@/hooks/web/useDesign';

  export default defineComponent({
    name: 'TableHeaderCell',
    components: {
      EditTableHeaderCell,
      BasicHelp,
    },
    props: {
      column: {
        type: Object as PropType<BasicColumn>,
        default: () => ({}),
      },
    },
    setup(props) {
      const { prefixCls } = useDesign('basic-table-header-cell');

      const getIsEdit = computed(() => !!props.column?.edit);
      const getTitle = computed(() => props.column?.customTitle || props.column?.title);
      const getHelpMessage = computed(() => props.column?.helpMessage);

      return { prefixCls, getIsEdit, getTitle, getHelpMessage };
    },
  });
</script>
<style lang="less">
  @prefix-cls: ~'@{namespace}-basic-table-header-cell';

  .@{prefix-cls} {
    &__help {
      margin-left: 8px;
      color: rgb(0 0 0 / 65%) !important;
    }
  }
</style>

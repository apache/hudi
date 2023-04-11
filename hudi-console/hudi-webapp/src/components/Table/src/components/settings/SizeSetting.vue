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
  <Tooltip placement="top">
    <template #title>
      <span>{{ t('component.table.settingDens') }}</span>
    </template>

    <Dropdown placement="bottom" :trigger="['click']" :getPopupContainer="getPopupContainer">
      <ColumnHeightOutlined />
      <template #overlay>
        <Menu @click="handleTitleClick" selectable v-model:selectedKeys="selectedKeysRef">
          <MenuItem key="default">
            <span>{{ t('component.table.settingDensDefault') }}</span>
          </MenuItem>
          <MenuItem key="middle">
            <span>{{ t('component.table.settingDensMiddle') }}</span>
          </MenuItem>
          <MenuItem key="small">
            <span>{{ t('component.table.settingDensSmall') }}</span>
          </MenuItem>
        </Menu>
      </template>
    </Dropdown>
  </Tooltip>
</template>
<script lang="ts">
  import type { SizeType } from '../../types/table';
  import { defineComponent, ref } from 'vue';
  import { Tooltip, Dropdown, Menu } from 'ant-design-vue';
  import { ColumnHeightOutlined } from '@ant-design/icons-vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useTableContext } from '../../hooks/useTableContext';
  import { getPopupContainer } from '/@/utils';

  export default defineComponent({
    name: 'SizeSetting',
    components: {
      ColumnHeightOutlined,
      Tooltip,
      Dropdown,
      Menu,
      MenuItem: Menu.Item,
    },
    setup() {
      const table = useTableContext();
      const { t } = useI18n();

      const selectedKeysRef = ref<SizeType[]>([table.getSize()]);

      function handleTitleClick({ key }) {
        selectedKeysRef.value = [key];
        table.setProps({
          size: key,
        });
      }

      return {
        handleTitleClick,
        selectedKeysRef,
        getPopupContainer,
        t,
      };
    },
  });
</script>

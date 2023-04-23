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
  <div :class="bem()" class="flex px-2 py-1.5 items-center">
    <slot name="headerTitle" v-if="slots.headerTitle"></slot>
    <BasicTitle :helpMessage="helpMessage" v-if="!slots.headerTitle && title">
      {{ title }}
    </BasicTitle>
    <div
      class="flex items-center flex-1 cursor-pointer justify-self-stretch"
      v-if="search || toolbar"
    >
      <div :class="getInputSearchCls" v-if="search">
        <InputSearch
          :placeholder="t('common.searchText')"
          size="small"
          allowClear
          v-model:value="searchValue"
        />
      </div>
      <Dropdown @click.prevent v-if="toolbar">
        <Icon icon="ion:ellipsis-vertical" />
        <template #overlay>
          <Menu @click="handleMenuClick">
            <template v-for="item in toolbarList" :key="item.value">
              <MenuItem v-bind="{ key: item.value }">
                {{ item.label }}
              </MenuItem>
              <MenuDivider v-if="item.divider" />
            </template>
          </Menu>
        </template>
      </Dropdown>
    </div>
  </div>
</template>
<script lang="ts" setup>
  import { computed, ref, watch, useSlots } from 'vue';
  import { Dropdown, Menu, MenuItem, MenuDivider, InputSearch } from 'ant-design-vue';
  import { Icon } from '/@/components/Icon';
  import { BasicTitle } from '/@/components/Basic';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useDebounceFn } from '@vueuse/core';
  import { createBEM } from '/@/utils/bem';
  import { ToolbarEnum } from '../types/tree';

  const searchValue = ref('');

  const [bem] = createBEM('tree-header');

  const props = defineProps({
    helpMessage: {
      type: [String, Array] as PropType<string | string[]>,
      default: '',
    },
    title: {
      type: String,
      default: '',
    },
    toolbar: {
      type: Boolean,
      default: false,
    },
    checkable: {
      type: Boolean,
      default: false,
    },
    search: {
      type: Boolean,
      default: false,
    },
    searchText: {
      type: String,
      default: '',
    },
    checkAll: {
      type: Function,
      default: undefined,
    },
    expandAll: {
      type: Function,
      default: undefined,
    },
  } as const);
  const emit = defineEmits(['strictly-change', 'search']);

  const slots = useSlots();
  const { t } = useI18n();

  const getInputSearchCls = computed(() => {
    const titleExists = slots.headerTitle || props.title;
    return [
      'mr-1',
      'w-full',
      {
        ['ml-5']: titleExists,
      },
    ];
  });

  const toolbarList = computed(() => {
    const { checkable } = props;
    const defaultToolbarList = [
      { label: t('component.tree.expandAll'), value: ToolbarEnum.EXPAND_ALL },
      {
        label: t('component.tree.unExpandAll'),
        value: ToolbarEnum.UN_EXPAND_ALL,
        divider: checkable,
      },
    ];

    return checkable
      ? [
          { label: t('component.tree.selectAll'), value: ToolbarEnum.SELECT_ALL },
          {
            label: t('component.tree.unSelectAll'),
            value: ToolbarEnum.UN_SELECT_ALL,
            divider: checkable,
          },
          ...defaultToolbarList,
          // { label: t('component.tree.checkStrictly'), value: ToolbarEnum.CHECK_STRICTLY },
          // { label: t('component.tree.checkUnStrictly'), value: ToolbarEnum.CHECK_UN_STRICTLY },
        ]
      : defaultToolbarList;
  });

  function handleMenuClick(e) {
    const { key } = e;
    switch (key) {
      case ToolbarEnum.SELECT_ALL:
        props.checkAll?.(true);
        break;
      case ToolbarEnum.UN_SELECT_ALL:
        props.checkAll?.(false);
        break;
      case ToolbarEnum.EXPAND_ALL:
        props.expandAll?.(true);
        break;
      case ToolbarEnum.UN_EXPAND_ALL:
        props.expandAll?.(false);
        break;
      case ToolbarEnum.CHECK_STRICTLY:
        emit('strictly-change', false);
        break;
      case ToolbarEnum.CHECK_UN_STRICTLY:
        emit('strictly-change', true);
        break;
    }
  }

  function emitChange(value?: string): void {
    emit('search', value);
  }

  const debounceEmitChange = useDebounceFn(emitChange, 200);

  watch(
    () => searchValue.value,
    (v) => {
      debounceEmitChange(v);
    },
  );

  watch(
    () => props.searchText,
    (v) => {
      if (v !== searchValue.value) {
        searchValue.value = v;
      }
    },
  );
</script>

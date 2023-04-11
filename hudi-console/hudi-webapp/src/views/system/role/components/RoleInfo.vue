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
  <BasicDrawer
    :showOkBtn="false"
    @register="registerDrawer"
    showFooter
    width="40%"
    @ok="handleSubmit"
  >
    <template #title>
      <Icon icon="ant-design:smile-outlined" />
      {{ t('system.role.roleInfo') }}
    </template>
    <Description :column="1" :data="roleInfo" :schema="roleColumn" />
    <p class="mt-20px">
      <Icon icon="ant-design:trophy-outlined" />&nbsp;&nbsp; {{ t('system.role.assignment') }}
      <Tree
        :check-strictly="false"
        :checkable="true"
        :defaultExpandedKeys="checkedKeys"
        v-model:checkedKeys="checkedKeys"
        v-if="menuTreeData.length > 0"
        :tree-data="menuTreeData"
      >
        <template #title="{ text }">
          {{ handleTreeTitle(text) }}
        </template>
      </Tree>
    </p>
  </BasicDrawer>
</template>
<script lang="ts">
  export default defineComponent({
    name: 'RoleInfo',
  });
</script>

<script setup lang="ts">
  import { defineComponent, h, ref } from 'vue';
  import { Tree } from 'ant-design-vue';
  import { Description } from '/@/components/Description';
  import Icon from '/@/components/Icon';
  import { useDrawerInner, BasicDrawer } from '/@/components/Drawer';
  import { getMenuList, getRoleMenu } from '/@/api/base/system';
  import { DataNode } from 'ant-design-vue/lib/tree';
  import { useI18n } from '/@/hooks/web/useI18n';
  const { t } = useI18n();
  const roleInfo = ref<Recordable>({});
  const checkedKeys = ref<Array<string>>([]);
  const menuTreeData = ref<Array<DataNode>>([]);
  const leftNodes = ref<Array<string>>([]);

  const [registerDrawer, { closeDrawer }] = useDrawerInner((data: Recordable) => {
    data && onReceiveModalData(data);
  });

  // The default parent node is "/"
  function deepList(data) {
    data.map((item) => {
      if (item.children && item.children.length > 0) {
        deepList(item.children);
      } else {
        // Store all leaf nodes
        leftNodes.value.push(item.id);
      }
    });
  }

  async function onReceiveModalData(data) {
    roleInfo.value = Object.assign({}, data);
    const res = await getMenuList();
    // get all leaf nodes
    deepList(res?.rows?.children);

    const resp = await getRoleMenu({
      roleId: data.roleId,
    });
    const result = [...new Set(leftNodes.value)].filter((item) => new Set(resp).has(item));
    // Assign the result to the v-model bound property
    const selectedKey = [...result];
    checkedKeys.value = selectedKey;
    menuTreeData.value = res?.rows?.children;
  }

  const generatedLabelIcon = (icon: string, label: string) => {
    return h('div', null, [h(Icon, { icon }), h('span', { class: 'px-5px' }, label)]);
  };
  const roleColumn = [
    {
      label: generatedLabelIcon('ant-design:crown-outlined', t('system.role.form.roleName')),
      field: 'roleName',
    },
    {
      label: generatedLabelIcon('ant-design:book-outlined', t('common.description')),
      field: 'remark',
    },
    {
      label: generatedLabelIcon(`ant-design:clock-circle-outlined`, t('common.createTime')),
      field: 'createTime',
    },
    {
      label: generatedLabelIcon('ant-design:mail-outlined', t('common.modifyTime')),
      field: 'modifyTime',
      render: (curVal: string | undefined | null) => {
        if (curVal) {
          return curVal;
        } else {
          return t('system.role.modifyTime');
        }
      },
    },
  ];
  function handleSubmit() {
    closeDrawer();
  }
  function handleTreeTitle(text: string) {
    if (/^\w+\.\w+$/.test(text)) {
      return t(`menu.${text}`);
    }
    return text;
  }
</script>

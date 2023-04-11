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
    v-bind="$attrs"
    @register="registerDrawer"
    showFooter
    :title="getTitle"
    width="40%"
    @ok="handleSubmit"
  >
    <BasicForm @register="registerForm" :schemas="formSchemas">
      <template #menu="{ model, field }">
        <BasicTree
          :default-expand-level="1"
          v-model:value="model[field]"
          :treeData="treeData"
          :fieldNames="{ title: 'text', key: 'id' }"
          v-if="treeData.length > 0"
          @check="handleTreeCheck"
          checkable
          toolbar
          :title="t('system.role.assignment')"
        >
          <template #title="{ text }">
            {{ handleTreeTitle(text) }}
          </template>
        </BasicTree>
      </template>
    </BasicForm>
  </BasicDrawer>
</template>
<script lang="ts">
  import { defineComponent, ref, computed, unref, nextTick } from 'vue';
  import { BasicForm, FormSchema, useForm } from '/@/components/Form';
  import { handleRoleCheck } from '../role.data';
  import { BasicDrawer, useDrawerInner } from '/@/components/Drawer';
  import { BasicTree, TreeItem } from '/@/components/Tree';
  import { fetchRoleCreate, fetchRoleUpdate } from '/@/api/system/role';
  import { getMenuList, getRoleMenu } from '/@/api/base/system';
  import { FormTypeEnum } from '/@/enums/formEnum';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useI18n } from '/@/hooks/web/useI18n';

  const handleTreeIcon = (treeData: TreeItem[]): TreeItem[] => {
    if (!treeData?.length) {
      return [];
    }
    treeData.forEach((v) => {
      v.icon = v.icon && !v.icon.includes('ant-design:') ? `ant-design:${v.icon}-outlined` : v.icon;
      v.children && handleTreeIcon(v.children);
    });
    return treeData;
  };

  export default defineComponent({
    name: 'RoleDrawer',
    components: { BasicDrawer, BasicForm, BasicTree },
    emits: ['success', 'register'],
    setup(_, { emit }) {
      const { t } = useI18n();
      const formType = ref(FormTypeEnum.Edit);
      const treeData = ref<TreeItem[]>([]);
      let singleNodeKeys: string[] = [];
      let selectedKeysAndHalfCheckedKeys = ref<string[]>([]);

      const { createMessage } = useMessage();
      const isCreate = computed(() => unref(formType) === FormTypeEnum.Create);

      const formSchemas = computed((): FormSchema[] => {
        return [
          { field: 'roleId', label: '', component: 'Input', show: false },
          {
            field: 'roleName',
            label: t('system.role.form.roleName'),
            component: 'Input',
            componentProps: { disabled: !isCreate.value },
            rules: isCreate.value
              ? [{ required: true, validator: handleRoleCheck, trigger: 'blur' }]
              : [],
          },
          { label: t('common.description'), field: 'remark', component: 'InputTextArea' },
          {
            label: t('system.role.form.menuId'),
            field: 'menuId',
            slot: 'menu',
            component: 'Select',
            dynamicRules: () => [{ required: true, message: t('system.role.form.menuIdRequired') }],
          },
        ];
      });
      const [registerForm, { resetFields, setFieldsValue, validate }] = useForm({
        labelWidth: 120,
        colon: true,
        baseColProps: { span: 22 },
        showActionButtonGroup: false,
      });

      const [registerDrawer, { setDrawerProps, closeDrawer }] = useDrawerInner(async (data) => {
        // childrenNodeKeys = [];
        resetFields();
        setDrawerProps({
          confirmLoading: false,
          showFooter: data.formType !== FormTypeEnum.View,
        });

        formType.value = data.formType;

        function findSingleNode(data: Recordable) {
          data.map((item: Recordable) => {
            if (item.children && item.children.length > 0) {
              findSingleNode(item.children);
            } else {
              // Store all leaf nodes
              singleNodeKeys.push(item.id);
            }
          });
        }

        if (unref(treeData).length === 0) {
          const res = await getMenuList();
          treeData.value = handleTreeIcon(res?.rows?.children);
          findSingleNode(res?.rows?.children);
        }
        if (!unref(isCreate)) {
          const res = await getRoleMenu({ roleId: data.record.roleId });
          selectedKeysAndHalfCheckedKeys.value = res || [];
          const result = [...new Set(singleNodeKeys)].filter((item) => new Set(res).has(item));
          nextTick(() => {
            setFieldsValue({
              roleName: data.record.roleName,
              roleId: data.record.roleId,
              remark: data.record.remark,
              menuId: [...result],
            });
          });
        }
      });

      const getTitle = computed(() => {
        return {
          [FormTypeEnum.Create]: t('system.role.form.create'),
          [FormTypeEnum.Edit]: t('system.role.form.edit'),
          [FormTypeEnum.View]: t('system.role.form.view'),
        }[unref(formType)];
      });

      async function handleSubmit() {
        try {
          const values = await validate();
          // First, a simple judgment, does not contain app:view (home) this permission, the error is reported
          if (selectedKeysAndHalfCheckedKeys.value.indexOf('100067') < 0) {
            createMessage.warning(t('system.role.form.noViewPermission'));
            return;
          }
          setDrawerProps({ confirmLoading: true });
          const params = Object.assign({}, values, {
            menuId: selectedKeysAndHalfCheckedKeys.value.join(','),
          });

          !unref(isCreate) ? await fetchRoleUpdate(params) : await fetchRoleCreate(params);
          closeDrawer();
          emit('success');
        } catch (e) {
          console.log(e);
        } finally {
          setDrawerProps({ confirmLoading: false });
        }
      }
      function handleTreeCheck(checkedKeys: string[], e: any) {
        selectedKeysAndHalfCheckedKeys.value = [...checkedKeys, ...e.halfCheckedKeys];
      }
      function handleTreeTitle(text: string) {
        if (/^\w+\.\w+$/.test(text)) {
          return t(`menu.${text}`);
        }
        return text;
      }
      return {
        t,
        formSchemas,
        registerDrawer,
        registerForm,
        getTitle,
        handleSubmit,
        treeData,
        handleTreeCheck,
        handleTreeTitle,
      };
    },
  });
</script>

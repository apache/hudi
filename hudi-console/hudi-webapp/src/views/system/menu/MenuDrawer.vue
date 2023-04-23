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
    width="50%"
    @ok="handleSubmit"
  >
    <BasicForm @register="registerForm" />
  </BasicDrawer>
</template>
<script lang="ts">
  import { defineComponent, ref, computed, unref } from 'vue';
  import { BasicForm, useForm } from '/@/components/Form';
  import { formSchema, TypeEnum } from './menu.data';
  import { BasicDrawer, useDrawerInner } from '/@/components/Drawer';

  import { getMenuList } from '/@/api/base/system';
  import { addMenu, editMenu } from '/@/api/system/menu';

  export default defineComponent({
    name: 'MenuDrawer',
    components: { BasicDrawer, BasicForm },
    emits: ['success', 'register'],
    setup(_, { emit }) {
      const isUpdate = ref(true);

      const [registerForm, { resetFields, setFieldsValue, updateSchema, validate }] = useForm({
        labelWidth: 200,
        schemas: formSchema,
        showActionButtonGroup: false,
        colon: true,
        baseColProps: { span: 24 },
      });

      const [registerDrawer, { setDrawerProps, closeDrawer }] = useDrawerInner(async (data) => {
        resetFields();
        setDrawerProps({ confirmLoading: false });
        isUpdate.value = !!data?.isUpdate;
        console.log('data.record', data.record);
        if (unref(isUpdate)) {
          setFieldsValue({
            ...data.record,
            menuName: data.record.title,
            orderNum: data.record.order,
            perms: data.record.permission,
            menuId: data.record.id,
          });
        }
        const res = await getMenuList({ type: TypeEnum.Menu });
        updateSchema({
          field: 'parentId',
          componentProps: { treeData: res?.rows?.children },
        });
      });

      const getTitle = computed(() => (!unref(isUpdate) ? 'Add Menu' : 'Edit Menu'));

      async function handleSubmit() {
        try {
          const values = await validate();
          values.display = !!values.display;
          setDrawerProps({ confirmLoading: true });
          unref(isUpdate) ? await editMenu(values) : await addMenu(values);
          closeDrawer();
          emit('success');
        } finally {
          setDrawerProps({ confirmLoading: false });
        }
      }

      return { registerDrawer, registerForm, getTitle, handleSubmit };
    },
  });
</script>

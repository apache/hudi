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
    :okText="t('common.submitText')"
    @register="registerDrawer"
    showFooter
    :title="getTitle"
    width="40%"
    @ok="handleSubmit"
  >
    <BasicForm @register="registerForm" />
  </BasicDrawer>
</template>
<script lang="ts">
  import { defineComponent, ref, computed, unref } from 'vue';
  import { BasicForm, useForm } from '/@/components/Form';
  import { formSchema } from '../token.data';
  import { BasicDrawer, useDrawerInner } from '/@/components/Drawer';

  import { fetchTokenCreate } from '/@/api/system/token';
  import { useI18n } from '/@/hooks/web/useI18n';

  export default defineComponent({
    name: 'TokenDrawer',
    components: { BasicDrawer, BasicForm },
    emits: ['success', 'register'],
    setup(_, { emit }) {
      const isUpdate = ref(true);
      const { t } = useI18n();

      const [registerForm, { resetFields, setFieldsValue, validate }] = useForm({
        labelWidth: 120,
        colon: true,
        schemas: formSchema,
        showActionButtonGroup: false,
        baseColProps: { lg: 22, md: 22 },
      });

      const [registerDrawer, { setDrawerProps, closeDrawer }] = useDrawerInner(async (data) => {
        resetFields();
        setDrawerProps({ confirmLoading: false });
        isUpdate.value = !!data?.isUpdate;

        if (unref(isUpdate)) {
          setFieldsValue({
            ...data.record,
          });
        }
      });

      const getTitle = computed(() =>
        !unref(isUpdate) ? t('system.token.addToken') : t('system.token.modifyToken'),
      );

      async function handleSubmit() {
        try {
          const values = await validate();
          setDrawerProps({ confirmLoading: true });
          const res = await fetchTokenCreate(values);
          closeDrawer();
          emit('success', { isUpdate: unref(isUpdate), values: res });
        } finally {
          setDrawerProps({ confirmLoading: false });
        }
      }

      return { t, registerDrawer, registerForm, getTitle, handleSubmit };
    },
  });
</script>

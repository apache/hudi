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
  <BasicModal :width="600" :show-cancel-btn="false" @register="registerModal" @ok="closeModal">
    <template #title>
      <Icon icon="ant-design:user-add-outlined" />
      {{ t('system.user.userInfo') }}
    </template>
    <Description :column="1" :data="userInfo" :schema="userColumn" />
  </BasicModal>
</template>
<script lang="ts">
  export default defineComponent({
    name: 'UserModal',
  });
</script>

<script setup lang="ts">
  import { Tag } from 'ant-design-vue';
  import { defineComponent, h, ref } from 'vue';
  import { GenderEnum, StatusEnum } from '../user.data';
  import { DescItem, Description } from '/@/components/Description';
  import Icon from '/@/components/Icon';
  import { useModalInner, BasicModal } from '/@/components/Modal';
  import { useI18n } from '/@/hooks/web/useI18n';

  const userInfo = ref<Recordable>({});

  const { t } = useI18n();
  const [registerModal, { closeModal }] = useModalInner((data: Recordable) => {
    data && onReceiveModalData(data);
  });
  function onReceiveModalData(data: Recordable) {
    userInfo.value = Object.assign({}, userInfo.value, data);
  }
  // Dynamically generate label icons
  const generatedLabelIcon = (icon: string, label: string) => {
    return h('div', null, [
      h(Icon, { icon: `ant-design:${icon}-outlined` }),
      h('span', { class: 'px-5px' }, label),
    ]);
  };
  const userColumn: DescItem[] = [
    { label: generatedLabelIcon('user', t('system.user.form.userName')), field: 'username' },
    { label: generatedLabelIcon('star', t('system.user.form.userType')), field: 'userType' },
    {
      label: generatedLabelIcon('skin', t('system.user.form.gender')),
      field: 'sex',
      render: (curVal: string | number) => {
        const sexMap = {
          [GenderEnum.Male]: t('system.user.male'),
          [GenderEnum.Female]: t('system.user.female'),
          [GenderEnum.Other]: t('system.user.secret'),
        };
        return sexMap[curVal] || curVal;
      },
    },
    { label: generatedLabelIcon('mail', 'E-Mail'), field: 'email' },
    {
      label: generatedLabelIcon('smile', t('system.user.form.status')),
      field: 'status',
      render: (curVal: string | number) => {
        if (curVal == StatusEnum.Locked) {
          return h(Tag, { color: 'red' }, () => t('system.user.locked'));
        } else if (curVal == StatusEnum.Effective) {
          return h(Tag, { color: 'green' }, () => t('system.user.effective'));
        } else {
          return h('span', null, curVal);
        }
      },
    },
    {
      label: generatedLabelIcon(`clock-circle`, t('common.createTime')),
      field: 'createTime',
    },
    {
      label: generatedLabelIcon(`login`, t('system.user.form.lastLoginTime')),
      field: 'lastLoginTime',
    },
    {
      label: generatedLabelIcon(`message`, t('common.description')),
      field: 'description',
    },
  ];
</script>

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
<script lang="ts" setup>
  import { Tooltip } from 'ant-design-vue';
  import Icon from '/@/components/Icon';
  import { useModal } from '/@/components/Modal';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { createAsyncComponent } from '/@/utils/factory/createAsyncComponent';
  const LockAction = createAsyncComponent(() => import('./lock/LockModal.vue'));

  const [register, { openModal }] = useModal();
  const { t } = useI18n();
  function handleLock() {
    openModal(true);
  }
</script>
<template>
  <div>
    <Tooltip :title="t('layout.header.tooltipLock')" placement="bottom" :mouseEnterDelay="0.5">
      <Icon
        icon="ant-design:lock-outlined"
        class="!px-8px"
        style="font-size: 19px !important"
        @click="handleLock"
      />
    </Tooltip>
    <LockAction @register="register" />
  </div>
</template>

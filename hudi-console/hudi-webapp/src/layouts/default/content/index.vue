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
  <div :class="[prefixCls, getLayoutContentMode]" v-loading="getOpenPageLoading && getPageLoading">
    <PageLayout />
  </div>
</template>
<script lang="ts">
  import { defineComponent } from 'vue';
  import PageLayout from '/@/layouts/page/index.vue';
  import { useDesign } from '/@/hooks/web/useDesign';
  import { useRootSetting } from '/@/hooks/setting/useRootSetting';
  import { useTransitionSetting } from '/@/hooks/setting/useTransitionSetting';
  import { useContentViewHeight } from './useContentViewHeight';

  export default defineComponent({
    name: 'LayoutContent',
    components: { PageLayout },
    setup() {
      const { prefixCls } = useDesign('layout-content');
      const { getOpenPageLoading } = useTransitionSetting();
      const { getLayoutContentMode, getPageLoading } = useRootSetting();

      useContentViewHeight();
      return {
        prefixCls,
        getOpenPageLoading,
        getLayoutContentMode,
        getPageLoading,
      };
    },
  });
</script>
<style lang="less">
  @prefix-cls: ~'@{namespace}-layout-content';

  .@{prefix-cls} {
    position: relative;
    flex: 1 1 auto;
    min-height: 0;

    &.fixed {
      width: 1200px;
      margin: 0 auto;
    }

    &-loading {
      position: absolute;
      top: 200px;
      z-index: @page-loading-z-index;
    }
  }
</style>

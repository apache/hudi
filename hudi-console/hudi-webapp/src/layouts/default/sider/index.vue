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
  <Drawer
    v-if="getIsMobile"
    placement="left"
    :class="prefixCls"
    :width="getMenuWidth"
    :getContainer="null"
    :visible="!getCollapsed"
    @close="handleClose"
  >
    <Sider />
  </Drawer>
  <MixSider v-else-if="getIsMixSidebar" />
  <Sider v-else />
</template>
<script lang="ts">
  import { defineComponent } from 'vue';

  import Sider from './LayoutSider.vue';
  import MixSider from './MixSider.vue';
  import { Drawer } from 'ant-design-vue';

  import { useAppInject } from '/@/hooks/web/useAppInject';
  import { useMenuSetting } from '/@/hooks/setting/useMenuSetting';
  import { useDesign } from '/@/hooks/web/useDesign';
  export default defineComponent({
    name: 'SiderWrapper',
    components: { Sider, Drawer, MixSider },
    setup() {
      const { prefixCls } = useDesign('layout-sider-wrapper');
      const { getIsMobile } = useAppInject();
      const { setMenuSetting, getCollapsed, getMenuWidth, getIsMixSidebar } = useMenuSetting();

      function handleClose() {
        setMenuSetting({
          collapsed: true,
        });
      }

      return { prefixCls, getIsMobile, getCollapsed, handleClose, getMenuWidth, getIsMixSidebar };
    },
  });
</script>
<style lang="less">
  @prefix-cls: ~'@{namespace}-layout-sider-wrapper';

  .@{prefix-cls} {
    .ant-drawer-body {
      height: 100vh;
      padding: 0;
    }

    .ant-drawer-header-no-title {
      display: none;
    }
  }
</style>

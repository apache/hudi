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
  <div
    v-if="getMenuFixed && !getIsMobile"
    :style="getHiddenDomStyle"
    v-show="showClassSideBarRef"
  ></div>
  <Sider
    v-show="showClassSideBarRef"
    ref="sideRef"
    breakpoint="lg"
    collapsible
    :class="getSiderClass"
    :width="getMenuWidth"
    :collapsed="getCollapsed"
    :collapsedWidth="getCollapsedWidth"
    :theme="getMenuTheme"
    @breakpoint="onBreakpointChange"
    :trigger="getTrigger"
    v-bind="getTriggerAttr"
  >
    <template #trigger v-if="getShowTrigger">
      <LayoutTrigger />
    </template>
    <LayoutMenu :theme="getMenuTheme" :menuMode="getMode" :splitType="getSplitType" />
    <DragBar ref="dragBarRef" />
  </Sider>
</template>
<script lang="ts">
  import { computed, defineComponent, ref, unref, CSSProperties, h } from 'vue';

  import { Layout } from 'ant-design-vue';
  import LayoutMenu from '../menu/index.vue';
  import LayoutTrigger from '/@/layouts/default/trigger/index.vue';

  import { MenuModeEnum, MenuSplitTyeEnum } from '/@/enums/menuEnum';

  import { useMenuSetting } from '/@/hooks/setting/useMenuSetting';
  import { useTrigger, useDragLine, useSiderEvent } from './useLayoutSider';
  import { useAppInject } from '/@/hooks/web/useAppInject';
  import { useDesign } from '/@/hooks/web/useDesign';

  import DragBar from './DragBar.vue';
  export default defineComponent({
    name: 'LayoutSideBar',
    components: { Sider: Layout.Sider, LayoutMenu, DragBar, LayoutTrigger },
    setup() {
      const dragBarRef = ref<ElRef>(null);
      const sideRef = ref<ElRef>(null);

      const {
        getCollapsed,
        getMenuWidth,
        getSplit,
        getMenuTheme,
        getRealWidth,
        getMenuHidden,
        getMenuFixed,
        getIsMixMode,
        toggleCollapsed,
      } = useMenuSetting();

      const { prefixCls } = useDesign('layout-sideBar');

      const { getIsMobile } = useAppInject();

      const { getTriggerAttr, getShowTrigger } = useTrigger(getIsMobile);

      useDragLine(sideRef, dragBarRef);

      const { getCollapsedWidth, onBreakpointChange } = useSiderEvent();

      const getMode = computed(() => {
        return unref(getSplit) ? MenuModeEnum.INLINE : null;
      });

      const getSplitType = computed(() => {
        return unref(getSplit) ? MenuSplitTyeEnum.LEFT : MenuSplitTyeEnum.NONE;
      });

      const showClassSideBarRef = computed(() => {
        return unref(getSplit) ? !unref(getMenuHidden) : true;
      });

      const getSiderClass = computed(() => {
        return [
          prefixCls,
          {
            [`${prefixCls}--fixed`]: unref(getMenuFixed),
            [`${prefixCls}--mix`]: unref(getIsMixMode) && !unref(getIsMobile),
          },
        ];
      });

      const getHiddenDomStyle = computed((): CSSProperties => {
        const width = `${unref(getRealWidth)}px`;
        return {
          width: width,
          overflow: 'hidden',
          flex: `0 0 ${width}`,
          maxWidth: width,
          minWidth: width,
          transition: 'all 0.2s',
        };
      });

      // Using a calculated amount here may cause a sider exception
      // After the andv update, the code here can be deprecated if the trigger slot is available
      const getTrigger = h(LayoutTrigger);

      return {
        prefixCls,
        sideRef,
        dragBarRef,
        getIsMobile,
        getHiddenDomStyle,
        getSiderClass,
        getTrigger,
        getTriggerAttr,
        getCollapsedWidth,
        getMenuFixed,
        showClassSideBarRef,
        getMenuWidth,
        getCollapsed,
        getMenuTheme,
        onBreakpointChange,
        getMode,
        getSplitType,
        getShowTrigger,
        toggleCollapsed,
      };
    },
  });
</script>
<style lang="less">
  @prefix-cls: ~'@{namespace}-layout-sideBar';

  .@{prefix-cls} {
    z-index: @layout-sider-fixed-z-index;

    &--fixed {
      position: fixed;
      top: 0;
      left: 0;
      height: 100%;
    }

    &--mix {
      top: @header-height;
      height: calc(100% - @header-height);
    }

    &.ant-layout-sider-dark {
      background-color: @sider-dark-bg-color;

      .ant-layout-sider-trigger {
        color: darken(@white, 25%);
        background-color: @trigger-dark-bg-color;

        &:hover {
          color: @white;
          background-color: @trigger-dark-hover-bg-color;
        }
      }
    }

    &:not(.ant-layout-sider-dark) {
      // box-shadow: 2px 0 8px 0 rgba(29, 35, 41, 0.05);

      .ant-layout-sider-trigger {
        color: @text-color-base;
        border-top: 1px solid @border-color-light;
      }
    }

    .ant-layout-sider-zero-width-trigger {
      top: 40%;
      z-index: 10;
    }

    & .ant-layout-sider-trigger {
      height: 36px;
      line-height: 36px;
    }
  }
</style>

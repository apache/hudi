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
  <div v-if="getShowDarkModeToggle" :class="getClass" @click="toggleDarkMode">
    <div :class="`${prefixCls}-inner`"></div>
    <SvgIcon size="14" name="sun" />
    <SvgIcon size="14" name="moon" />
  </div>
</template>
<script lang="ts" setup>
  import { computed, unref } from 'vue';
  import { SvgIcon } from '/@/components/Icon';
  import { useDesign } from '/@/hooks/web/useDesign';
  import { useRootSetting } from '/@/hooks/setting/useRootSetting';
  import { updateHeaderBgColor, updateSidebarBgColor } from '/@/logics/theme/updateBackground';
  import { updateDarkTheme } from '/@/logics/theme/dark';
  import { ThemeEnum } from '/@/enums/appEnum';

  const { prefixCls } = useDesign('dark-switch');
  const { getDarkMode, setDarkMode, getShowDarkModeToggle } = useRootSetting();

  const isDark = computed(() => getDarkMode.value === ThemeEnum.DARK);

  const getClass = computed(() => [
    prefixCls,
    {
      [`${prefixCls}--dark`]: unref(isDark),
    },
  ]);

  function toggleDarkMode() {
    const darkMode = getDarkMode.value === ThemeEnum.DARK ? ThemeEnum.LIGHT : ThemeEnum.DARK;
    setDarkMode(darkMode);
    updateDarkTheme(darkMode);
    updateHeaderBgColor();
    updateSidebarBgColor();
  }
</script>
<style lang="less" scoped>
  @prefix-cls: ~'@{namespace}-dark-switch';

  html[data-theme='dark'] {
    .@{prefix-cls} {
      border: 1px solid rgb(196 188 188);
    }
  }

  .@{prefix-cls} {
    position: relative;
    display: flex;
    width: 50px;
    height: 26px;
    padding: 0 6px;
    margin-left: auto;
    cursor: pointer;
    background-color: #151515;
    border-radius: 30px;
    justify-content: space-between;
    align-items: center;

    &-inner {
      position: absolute;
      z-index: 1;
      width: 18px;
      height: 18px;
      background-color: #fff;
      border-radius: 50%;
      transition: transform 0.5s, background-color 0.5s;
      will-change: transform;
    }

    &--dark {
      .@{prefix-cls}-inner {
        transform: translateX(calc(100% + 2px));
      }
    }
  }
</style>

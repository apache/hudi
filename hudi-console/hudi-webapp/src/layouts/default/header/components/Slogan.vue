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
<script lang="ts" setup name="Slogan">
  import { computed } from 'vue';
  import { useMenuSetting } from '/@/hooks/setting/useMenuSetting';
  import { useDesign } from '/@/hooks/web/useDesign';

  const { prefixCls } = useDesign('slogan');
  const { getCollapsed } = useMenuSetting();
  const slognClass = computed(() => [
    prefixCls,
    {
      'collapse-slogo': getCollapsed.value,
    },
  ]);
</script>
<template>
  <div :class="slognClass">
    <span :class="`${prefixCls}-streampark`">
      Apache Hudi,&nbsp; Make&nbsp; stream processing&nbsp;easier!
    </span>
  </div>
</template>
<style lang="less">
  @prefix-cls: ~'@{namespace}-slogan';

  .@{prefix-cls} {
    font-size: 1.5rem;
    font-weight: bolder;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial,
      'Noto Sans', sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol',
      'Noto Color Emoji';
    overflow: hidden;
    line-height: @header-height;
    height: @header-height;
    max-width: 620px;

    &-streampark {
      background: linear-gradient(130deg, #24c6dc, #5433ff 41.07%, #f09 76.05%);
      -webkit-text-fill-color: transparent;
      -webkit-background-clip: text;
      -webkit-box-decoration-break: clone;
      text-overflow: ellipsis;
      overflow: hidden;
      white-space: nowrap;
      width: 100%;
      display: inline-block;

      &::selection,
      span::selection {
        -webkit-text-fill-color: #fff;
      }
    }
  }

  @media screen and (max-width: 1670px) {
    .@{prefix-cls}:not(.collapse-slogo) {
      display: none;
    }
  }
  @media screen and (max-width: 1460px) {
    .@{prefix-cls}.collapse-slogo {
      display: none;
    }
  }
</style>

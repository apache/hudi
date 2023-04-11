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
  <span :class="`${prefixCls}__extra-fold`" @click="handleFold">
    <Icon :icon="getIcon" />
  </span>
</template>
<script lang="ts">
  import { defineComponent, unref, computed } from 'vue';
  import { Icon } from '/@/components/Icon';

  import { useDesign } from '/@/hooks/web/useDesign';
  import { useHeaderSetting } from '/@/hooks/setting/useHeaderSetting';
  import { useMenuSetting } from '/@/hooks/setting/useMenuSetting';
  import { triggerWindowResize } from '/@/utils/event';

  export default defineComponent({
    name: 'FoldButton',
    components: { Icon },
    setup() {
      const { prefixCls } = useDesign('multiple-tabs-content');
      const { getShowMenu, setMenuSetting } = useMenuSetting();
      const { getShowHeader, setHeaderSetting } = useHeaderSetting();

      const getIsUnFold = computed(() => !unref(getShowMenu) && !unref(getShowHeader));

      const getIcon = computed(() =>
        unref(getIsUnFold) ? 'codicon:screen-normal' : 'codicon:screen-full',
      );

      function handleFold() {
        const isUnFold = unref(getIsUnFold);
        setMenuSetting({
          show: isUnFold,
          hidden: !isUnFold,
        });
        setHeaderSetting({ show: isUnFold });
        triggerWindowResize();
      }

      return { prefixCls, getIcon, handleFold };
    },
  });
</script>

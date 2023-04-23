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
  <Dropdown
    :dropMenuList="getDropMenuList"
    :trigger="getTrigger"
    placement="bottom"
    overlayClassName="multiple-tabs__dropdown"
    @menu-event="handleMenuEvent"
  >
    <div :class="`${prefixCls}__info`" @contextmenu="handleContext" v-if="getIsTabs">
      <span class="ml-1">{{ getTitle }}</span>
    </div>
    <span :class="`${prefixCls}__extra-quick`" v-else @click="handleContext">
      <Icon icon="ion:chevron-down" />
    </span>
  </Dropdown>
</template>
<script lang="ts">
  import type { PropType } from 'vue';
  import type { RouteLocationNormalized } from 'vue-router';

  import { defineComponent, computed, unref } from 'vue';
  import { Dropdown } from '/@/components/Dropdown';
  import { Icon } from '/@/components/Icon';

  import { TabContentProps } from '../types';

  import { useDesign } from '/@/hooks/web/useDesign';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useTabDropdown } from '../useTabDropdown';

  export default defineComponent({
    name: 'TabContent',
    components: { Dropdown, Icon },
    props: {
      tabItem: {
        type: Object as PropType<RouteLocationNormalized>,
        default: null,
      },
      isExtra: Boolean,
    },
    setup(props) {
      const { prefixCls } = useDesign('multiple-tabs-content');
      const { t } = useI18n();

      const getTitle = computed(() => {
        const { tabItem: { meta, name } = {} } = props;
        return t((name || meta?.title) as string);
      });

      const getIsTabs = computed(() => !props.isExtra);

      const getTrigger = computed((): ('contextmenu' | 'click' | 'hover')[] =>
        unref(getIsTabs) ? ['contextmenu'] : ['click'],
      );

      const { getDropMenuList, handleMenuEvent, handleContextMenu } = useTabDropdown(
        props as TabContentProps,
        getIsTabs,
      );

      function handleContext(e) {
        props.tabItem && handleContextMenu(props.tabItem)(e);
      }

      return {
        prefixCls,
        getDropMenuList,
        handleMenuEvent,
        handleContext,
        getTrigger,
        getIsTabs,
        getTitle,
      };
    },
  });
</script>

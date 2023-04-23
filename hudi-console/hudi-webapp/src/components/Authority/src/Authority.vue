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
<script lang="ts">
  import type { PropType } from 'vue';
  import { defineComponent } from 'vue';
  import { RoleEnum } from '/@/enums/roleEnum';
  import { usePermission } from '/@/hooks/web/usePermission';
  import { getSlot } from '/@/utils/helper/tsxHelper';

  export default defineComponent({
    name: 'Authority',
    props: {
      /**
       * Specified role is visible
       * When the permission mode is the role mode, the value value can pass the role value.
       * When the permission mode is background, the value value can pass the code permission value
       * @default ''
       */
      value: {
        type: [Number, Array, String] as PropType<RoleEnum | RoleEnum[] | string | string[]>,
        default: '',
      },
    },
    setup(props, { slots }) {
      const { hasPermission } = usePermission();

      /**
       * Render role button
       */
      function renderAuth() {
        const { value } = props;
        if (!value) {
          return getSlot(slots);
        }
        return hasPermission(value) ? getSlot(slots) : null;
      }

      return () => {
        // Role-based value control
        return renderAuth();
      };
    },
  });
</script>

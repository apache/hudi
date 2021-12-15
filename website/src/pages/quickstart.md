---
id: quickstart
title: quickstart
---

import {Route} from '@docusaurus/router';

<Route
path={'/*'}
component={() => {
global.window && (global.window.location.href = '/docs/quick-start-guide');
return null;
}}
/>

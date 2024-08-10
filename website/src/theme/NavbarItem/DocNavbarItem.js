import React from 'react';
import {useActiveDocContext} from '@docusaurus/plugin-content-docs/client';
import {useLayoutDoc} from '@docusaurus/theme-common/internal';
import DefaultNavbarItem from '@theme/NavbarItem/DefaultNavbarItem';
export default function DocNavbarItem({
  docId,
  label: staticLabel,
  docsPluginId,
  ...props
}) {
  const {activeDoc} = useActiveDocContext(docsPluginId);
  const doc = useLayoutDoc(docId, docsPluginId);
  const pageActive = activeDoc?.path === doc?.path;
  // Draft and unlisted items are not displayed in the navbar.
  if (doc === null || (doc.unlisted && !pageActive)) {
    return null;
  }
  return (
    <DefaultNavbarItem
      exact
      {...props}
      isActive={() =>
        pageActive ||
        (!!activeDoc?.sidebar && activeDoc.sidebar === doc.sidebar)
      }
      label={staticLabel ?? doc.id}
      to={doc.path}
    />
  );
}

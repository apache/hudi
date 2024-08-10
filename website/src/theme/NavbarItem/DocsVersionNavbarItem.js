import React from 'react';
import {useDocsVersionCandidates} from '@docusaurus/theme-common/internal';
import DefaultNavbarItem from '@theme/NavbarItem/DefaultNavbarItem';
const getVersionMainDoc = (version) =>
  version.docs.find((doc) => doc.id === version.mainDocId);
export default function DocsVersionNavbarItem({
  label: staticLabel,
  to: staticTo,
  docsPluginId,
  ...props
}) {
  const version = useDocsVersionCandidates(docsPluginId)[0];
  const label = staticLabel ?? version.label;
  const path = staticTo ?? getVersionMainDoc(version).path;
  return <DefaultNavbarItem {...props} label={label} to={path} />;
}

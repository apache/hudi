import React from 'react';
import clsx from 'clsx';
import {HtmlClassNameProvider, ThemeClassNames} from '@docusaurus/theme-common';
import {
  DocsSidebarProvider,
  useDocRootMetadata,
} from '@docusaurus/plugin-content-docs/client';
import DocRootLayout from '@theme/DocRoot/Layout';
import NotFoundContent from '@theme/NotFound/Content';

const arrayOfPages = (matchPath) => [`${matchPath}/configurations`, `${matchPath}/basic_configurations`, `${matchPath}/timeline`, `${matchPath}/table_types`, `${matchPath}/migration_guide`, `${matchPath}/compaction`, `${matchPath}/clustering`, `${matchPath}/indexing`, `${matchPath}/metadata`, `${matchPath}/metadata_indexing`, `${matchPath}/record_payload`, `${matchPath}/file_sizing`, `${matchPath}/hoodie_cleaner`, `${matchPath}/concurrency_control`, , `${matchPath}/write_operations`, `${matchPath}/key_generation`];
const showCustomStylesForDocs = (matchPath, pathname) => arrayOfPages(matchPath).includes(pathname);

export default function DocRoot(props) {
  const currentDocRouteMetadata = useDocRootMetadata(props);
  if (!currentDocRouteMetadata) {
    // We only render the not found content to avoid a double layout
    // see https://github.com/facebook/docusaurus/pull/7966#pullrequestreview-1077276692
    return <NotFoundContent />;
  }
  const {docElement, sidebarName, sidebarItems} = currentDocRouteMetadata;
  const addCustomClass = showCustomStylesForDocs(props.match.path, props.location.pathname)
  return (
    <HtmlClassNameProvider className={clsx(ThemeClassNames.page.docsDocPage, {'docs-custom-styles': addCustomClass })}>
      <DocsSidebarProvider name={sidebarName} items={sidebarItems}>
        <DocRootLayout>{docElement}</DocRootLayout>
      </DocsSidebarProvider>
    </HtmlClassNameProvider>
  );
}

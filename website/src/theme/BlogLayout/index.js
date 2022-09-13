/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import BlogSidebar from '@theme/BlogSidebar';

function BlogLayout(props) {
  const {sidebar, toc, children, ...layoutProps} = props;
  const hasSidebar = sidebar && sidebar.items.length > 0;
  const isBlogListPage = props.pageClassName === "blog-list-page";
  const isTagsPostList = props.pageClassName === "blog-tags-post-list-page";
  return (
    <Layout {...layoutProps}>
      <div className="container margin-vert--lg">
        <div className="row">
          {hasSidebar && (
            <aside className="col col--3">
              <BlogSidebar sidebar={sidebar} />
            </aside>
          )}
          <main
            className={clsx('col', {
              'col--7': hasSidebar,
              'col--9 col--offset-2': !hasSidebar,
              'row': isBlogListPage,
              'tags-post-list': isTagsPostList
            })}
            itemScope
            itemType="http://schema.org/Blog">
            {children}
          </main>
          {toc && <div className="col col--2">{toc}</div>}
        </div>
      </div>
    </Layout>
  );
}

export default BlogLayout;

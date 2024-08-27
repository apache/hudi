import React from 'react';
import BlogPostItemHeaderTitle from '@theme/BlogPostItem/Header/Title';
import BlogPostItemHeaderInfo from '@theme/BlogPostItem/Header/Info';
import clsx from "clsx";
import TagsListInline from "@theme/TagsListInline";
import {useBlogPost} from "@docusaurus/plugin-content-blog/client";
import {ThemeClassNames} from "@docusaurus/theme-common";

export default function BlogPostItemHeader() {
    const {metadata, isBlogPostPage} = useBlogPost();
    const {
        tags,
        hasTruncateMarker,
    } = metadata;
    // A post is truncated if it's in the "list view" and it has a truncate marker
    const truncatedPost = !isBlogPostPage && hasTruncateMarker;
    const tagsExists = tags.length > 0;
  return (
    <header>
      <BlogPostItemHeaderTitle />
      <BlogPostItemHeaderInfo />
        {tagsExists && (
            <div
                className={clsx(
                    'row tagsListInline',
                    'margin-top--sm',
                    ThemeClassNames.blog.blogFooterEditMetaRow,
                )}>
                <div className="col">
                    <TagsListInline tags={tags} />
                </div>
            </div>
        )}
    </header>
  );
}

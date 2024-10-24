import React from "react";
import Layout from "@theme/Layout";
import { PageMetadata } from "@docusaurus/theme-common";
import { useBlogTagsPostsPageTitle } from "@docusaurus/theme-common/internal";
import BlogListPaginator from "@theme/BlogListPaginator";
import SearchMetadata from "@theme/SearchMetadata";
import BlogTabs from "../../components/BlogTabs";

function BlogTagsPostsPageMetadata({ tag }) {
  const title = useBlogTagsPostsPageTitle(tag);
  return (
    <>
      <PageMetadata title={title} description={tag.description} />
      <SearchMetadata tag="blog_tags_posts" />
    </>
  );
}

function BlogTagsPostsPageContent({ items, listMetadata, tag }) {
  return (
    <div className="container" style={{ marginBottom: 16 }}>
      <BlogTabs isTagPage items={items} tag={tag} />
      <BlogListPaginator metadata={listMetadata} />
    </div>
  );
}

export default function BlogTagsPostsPageWrapper(props) {
  return (
    <Layout title="Blog">
      <BlogTagsPostsPageMetadata {...props} />
      <BlogTagsPostsPageContent {...props} />
    </Layout>
  );
}

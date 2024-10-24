import React, { useEffect, useState } from "react";
import clsx from "clsx";
import BlogPostItems from "@theme/BlogPostItems";
import { useHistory } from "@docusaurus/router";
import Heading from "@theme/Heading";
import Link from "@docusaurus/Link";
import Translate from "@docusaurus/Translate";
import { useBlogTagsPostsPageTitle } from "@docusaurus/theme-common/internal";
import Unlisted from "@theme/ContentVisibility/Unlisted";



const NotFoundContent = () => {
  return (
    <div className="text-center text-muted blog-not-found">
      <h2>No blog posts found for this category.</h2>
    </div>
  );
};

function BlogTabs({ items, isTagPage, tag = {}, blogCategories }) {
  const history = useHistory();
  const title = useBlogTagsPostsPageTitle(tag);
  const urlParams = new URLSearchParams(window.location.search);
  const category = urlParams.get("category");
  const [selectedTab, setSelectedTab] = useState(category || "all");

  useEffect(() => {
    if (!category) {
      setSelectedTab("all");
    } else {
      setSelectedTab(category);
    }
  }, [category]);

  return (
    <div className="mb-5">
      {isTagPage ? (
        <>
          {tag?.unlisted && <Unlisted />}
          <header className="margin-bottom--lg margin-top--lg">
            <Heading as="h1">{title}</Heading>
            {tag.description && <p>{tag.description}</p>}
            <Link href={tag?.allTagsPath}>
              <Translate
                id="theme.tags.tagsPageLink"
                description="The label of the link targeting the tag list page"
              >
                View All Tags
              </Translate>
            </Link>
          </header>
        </>
      ) : (
        <ul className="tabs">
          {blogCategories.filter(d => d.show).map((category) => (
            <li
              key={category.value}
              className={clsx("tab", {
                ["tab-active"]: selectedTab === category.value,
              })}
              onClick={() => {
                setSelectedTab(category.value);
                history.push(`/blog?category=${category.value}`);
              }}
            >
              {category.label}
            </li>
          ))}
        </ul>
      )}

      {items.length === 0 ? (
        <NotFoundContent />
      ) : (
        <div className="blogPostItemsWrapper">
          <BlogPostItems items={items} />{" "}
        </div>
      )}
    </div>
  );
}

export default BlogTabs;

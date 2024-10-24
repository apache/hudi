import React, { useState, useEffect, useMemo } from "react";
import Layout from "@theme/Layout";
import BlogListPaginator from "@theme/BlogListPaginator";
import BlogTabs from "../../components/BlogTabs";
import { useLocation, useHistory } from "@docusaurus/router";
import BlogListPageBanner from "../../components/BlogListPageBanner";
import BlogListPagination from "../../components/BlogListPagination";

function BlogPage(props) {
  const location = useLocation();
  const history = useHistory();

  const [currentPage, setCurrentPage] = useState(1);
  const postsPerPage = 9;

  // Get query params from URL (default to 'All' if no category in URL)
  const queryParams = new URLSearchParams(location.search);
  const initialCategory = queryParams.get("category") || "all";

  const [selectedCategory, setSelectedCategory] = useState(initialCategory);

  /**
   * Generates a list of blog post objects from the given context.
   *
   * @param {Function} ctx - The require.context function.
   * @param {string} type - The type to assign in the front matter (e.g., "link" or "page").
   */
  const generateBlogPosts = (ctx, type, category) => {
    const blogpostNames = ctx.keys();
    return blogpostNames.reduce((blogposts, blogpostName) => {
      const module = ctx(blogpostName);
      return [
        ...blogposts,
        {
          frontMatter: { ...module.frontMatter, type, category },
          metadata: { ...module.metadata },
          assets: { ...module.assets },
        },
      ];
    }, []);
  };

  const allBlogPosts = generateBlogPosts(
    require.context("../../../blog", true),
    "page",
    { label: "User Story", urlKey: "user-stories" }
  );

  const allVideoPosts = generateBlogPosts(
    require.context("../../../videoBlog", true),
    "link",
    { label: "Guides & Tutorials", urlKey: "guide" }
  );

  const allUseCase = generateBlogPosts(
    require.context("../../../useCase", true),
    "page",
    { label: "Use Case", urlKey: "use-case" }
  );

  const allEngineering = generateBlogPosts(
    require.context("../../../engineering", true),
    "page",
    { label: "Engineering", urlKey: "engineering" }
  );

  const allTalksAndEvents = generateBlogPosts(
    require.context("../../../talks-events", true),
    "link",
    { label: "Talks & Events", urlKey: "talks-events" }
  );

  const latestBlog = useMemo(() => {
    const combinedArray = allBlogPosts.concat(allVideoPosts);
    combinedArray.sort(
      (a, b) => new Date(b.metadata.date) - new Date(a.metadata.date)
    );

    if (combinedArray.length > 0) {
      return combinedArray[0];
    }
    return [];
  }, [allVideoPosts, allBlogPosts]);

  const data = useMemo(() => {
    let combinedArray = [];
    if (!selectedCategory || selectedCategory === "all") {
      combinedArray = [
        ...allBlogPosts,
        ...allVideoPosts,
        ...allUseCase,
        ...allEngineering,
        ...allTalksAndEvents,
      ];
    }
    if (selectedCategory === "user-stories") {
      combinedArray = allBlogPosts;
    }
    if (selectedCategory === "guide") {
      combinedArray = allVideoPosts;
    }
    if (selectedCategory === "use-case") {
      combinedArray = allUseCase;
    }
    if (selectedCategory === "engineering") {
      combinedArray = allEngineering;
    }
    if (selectedCategory === "talks-events") {
      combinedArray = allTalksAndEvents;
    }
    combinedArray.sort(
      (a, b) => new Date(b.metadata.date) - new Date(a.metadata.date)
    );

    return combinedArray.filter(
      (data) => data.metadata.title !== latestBlog.metadata.title
    );
  }, [allVideoPosts, allBlogPosts, latestBlog]);

  // Update current page in the URL
  const handlePageChange = (page) => {
    setCurrentPage(page);
  };

  // Set the category and page from the URL on component mount
  useEffect(() => {
    const urlCategory = queryParams.get("category") || "all";
    setSelectedCategory(urlCategory);
    setCurrentPage(1);
  }, [location.search]);

  // Calculate pagination indexes
  const startIdx = (currentPage - 1) * postsPerPage;
  const endIdx = startIdx + postsPerPage;
  const paginatedItems = data.slice(startIdx, endIdx);
  const totalPages = Math.ceil(data?.length / postsPerPage);
  const blogCategories = [
    { label: "All categories", value: "all", show: !!data.length },
    { label: "User Story", value: "user-stories", show: !!allBlogPosts.length },
    { label: "Use Case", value: "use-case", show: !!allUseCase.length },
    {
      label: "Engineering",
      value: "engineering",
      show: !!allEngineering.length,
    },
    {
      label: "Guides & Tutorials",
      value: "guide",
      show: !!allVideoPosts.length,
    },
    {
      label: "Talks & Events",
      value: "talks-events",
      show: !!allTalksAndEvents.length,
    },
  ];

  return (
    <Layout title="Blog">
      <BlogListPageBanner blog={latestBlog} />
      <div className="container" style={{ marginBottom: 16 }}>
        <BlogTabs
          blogCategories={blogCategories}
          items={paginatedItems.map((d) => ({ content: d }))}
        />
        {totalPages > 1 && (
          <BlogListPagination
            numOfPages={totalPages}
            currentPage={currentPage}
            handlePageChange={handlePageChange}
          />
        )}
      </div>
    </Layout>
  );
}

export default BlogPage;

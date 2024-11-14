import React from "react";
import { useBaseUrlUtils } from "@docusaurus/core/lib/client/exports/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "../../theme/BlogPostItem/blogPostBoxStyles.module.css";
import AuthorName from "@site/src/components/AuthorName";

const BlogListPageBanner = ({ blog = {} }) => {
  const { metadata = {}, assets, frontMatter } = blog;

  const { withBaseUrl } = useBaseUrlUtils();
  const { permalink, title, date, authors } = metadata;

  const image =
    assets.image ?? frontMatter.image ?? "/assets/images/hudi-logo-medium.png";
  const dateObj = new Date(date);
  const options = { year: "numeric", month: "long", day: "numeric" };
  const formattedDate = dateObj.toLocaleDateString("en-US", options);
  return (
    <div className="blog-list-page-banner">
      <div className="container">
        <h1 className="our-blogs-title">
          <span className="our">Our</span> <span className="blogs">Blogs</span>
        </h1>
        <div className="row row--align-center">
          <div className="col col--5">
            <Link itemProp="url" to={permalink}>
              <img
                src={withBaseUrl(image, {
                  absolute: true,
                })}
                className="blog-banner-image"
                alt="blog-image"
              />
            </Link>
          </div>
          <div className="col col--7">
            <div className="blog-list-page-detail">
              <Link itemProp="url" to={permalink} className="blog-category">
                {frontMatter.category?.label}
              </Link>
              <h1 className="blog-list-page-title">{title}</h1>
              <div
                style={{ display: "flex", gap: "5px", alignItems: "center" }}
              >
                <AuthorName authors={authors} className={"auther"} />
                <p className="date">{formattedDate}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BlogListPageBanner;

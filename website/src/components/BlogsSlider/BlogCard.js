import React from "react";
import { useBaseUrlUtils } from "@docusaurus/core/lib/client/exports/useBaseUrl";
import Link from "@docusaurus/Link";

import AuthorName from "@site/src/components/AuthorName";
import ArrowRight from "./Icons/arrow-right.svg";
import styles from "./styles.module.css";

const BlogCard = ({ blog }) => {
  const { withBaseUrl } = useBaseUrlUtils();
  const { frontMatter, assets, metadata } = blog;
  const { date, title, authors, permalink } = metadata;
  const image = assets.image ?? frontMatter.image ?? "/assets/images/hudi.png";

  const dateObj = new Date(date);
  const options = { year: 'numeric', month: 'long', day: 'numeric' };
  const formattedDate = dateObj.toLocaleDateString('en-US', options);


  return (
    <div className={styles.blogsWrapper}>
      <Link itemProp="url" to={permalink} className={styles.link} target="_blank" rel="noopener noreferrer">
        <div className={styles.cardBlogs}>
          <div>
            <div className={styles.blogImgWrapper}>
              <img
                src={withBaseUrl(image, {
                  absolute: true,
                })}
                alt={title}
                className={styles.blogImg}
              />
            </div>
            <div className={styles.blogContent}>
              <AuthorName
                authors={authors}
                className={styles.authorName}
                withLink={false}
              />
              <div className={styles.date}>{formattedDate}</div>
            </div>
            <div className={styles.cardTitleWrapper}>
              <h5 className={styles.blogTitle}>{title}</h5>
            </div>
          </div>
        </div>
      </Link>
    </div>
  );
};
export default BlogCard;

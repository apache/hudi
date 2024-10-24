import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./blogPostBoxStyles.module.css";
import AuthorName from "@site/src/components/AuthorName";
import { useBaseUrlUtils } from "@docusaurus/core/lib/client/exports/useBaseUrl";
import Tag from "@theme/Tag";

export default function BlogPostBox({
  metadata = {},
  assets,
  frontMatter = {},
}) {
  const { withBaseUrl } = useBaseUrlUtils();
  const { date, permalink, tags, title, authors } = metadata;
  const { type, dateString } = frontMatter;

  const image =
    assets.image ?? frontMatter.image ?? "/assets/images/hudi-logo-medium.png";

  const manageVideoOpen = (videoLink) => {
    if (videoLink) {
      window.open(videoLink, "_blank", "noopener noreferrer");
    }
  };

  const tagsList = () => {
    return (
      <ul
        className={clsx(
          styles.tags,
          styles.authorTimeTags,
          "padding--none",
          "margin-left--sm"
        )}
      >
        {tags.map(({ label, permalink: tagPermalink }) => (
          <li key={tagPermalink} className={clsx(styles.tag)}>
            <Tag
              className={clsx(styles.greyLink)}
              label={label}
              permalink={tagPermalink}
            />
          </li>
        ))}
      </ul>
    );
  };

  function getStartOfMonth(datestring) {
    const year = +datestring?.split("-")[0];
    const month = +datestring?.split("-")[1];
    const date = new Date(year, month - 1, 1);

    const day = String(date.getDate()).padStart(2, "0");
    const monthFormatted = String(date.getMonth() + 1).padStart(2, "0");
    const yearFormatted = date.getFullYear();

    return `${monthFormatted}/${day}/${yearFormatted}`;
  }

  const AuthorsList = () => {
    const dateObj = dateString
      ? new Date(getStartOfMonth(dateString ?? ""))
      : new Date(date);
    const options = { year: "numeric", month: "long", day: "numeric" };
    const formattedDate = dateObj.toLocaleDateString("en-US", options);

    const authorsCount = authors.length;
    if (authorsCount === 0) {
      return (
        <div className={clsx(styles.authorTimeTags, "row 'margin-vert--md'")}>
          {formattedDate}
        </div>
      );
    }

    return (
      <div className={clsx(styles.authorTimeTags, "row 'margin-vert--md'")}>
        {formattedDate} by
        <AuthorName authors={authors} className={styles.authorsList} />
      </div>
    );
  };

  const renderPostHeader = () => {
    const TitleHeading = "h2";
    return (
      <header className={styles.postHeader}>
        <div>
          {image && (
            <div className="col blogThumbnail" itemProp="blogThumbnail">
              {type === "page" ? (
                <Link itemProp="url" to={permalink}>
                  <img
                    src={withBaseUrl(image, {
                      absolute: true,
                    })}
                    className="blog-image"
                  />
                </Link>
              ) : (
                <img
                  onClick={() => manageVideoOpen(frontMatter?.navigate)}
                  src={withBaseUrl(image, {
                    absolute: true,
                  })}
                  alt={title}
                  className={clsx(styles.videoImage, "blog-image")}
                />
              )}
            </div>
          )}
          <div className="blogInfo">
            <div>
              <Link
                itemProp="url"
                to={`/blog?category=${frontMatter.category?.urlKey}`}
                className={styles.categoryLink}
              >
                {frontMatter.category?.label}
              </Link>
            </div>
            <TitleHeading className={styles.blogPostTitle} itemProp="headline">
              {type === "blog" ? (
                <Link itemProp="url" to={permalink}>
                  <TitleHeading
                    className={styles.blogPostTitle}
                    itemProp="headline"
                  >
                    {title}
                  </TitleHeading>
                </Link>
              ) : (
                <TitleHeading
                  onClick={() => manageVideoOpen(frontMatter?.navigate)}
                  className={styles.blogPostTitle}
                  itemProp="headline"
                >
                  {title}
                </TitleHeading>
              )}
            </TitleHeading>
            <div
              className={clsx(
                styles.blogInfo,
                "margin-top--sm margin-bottom--sm"
              )}
            >
              {AuthorsList()}
            </div>
          </div>
        </div>
        {/*{!!tags.length && tagsList()}*/}
      </header>
    );
  };

  return (
    <article
      className="blog-list-item"
      itemProp="blogPost"
      itemScope
      itemType="http://schema.org/BlogPosting"
    >
      {renderPostHeader()}
    </article>
  );
}

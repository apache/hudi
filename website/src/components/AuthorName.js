import React from "react";
import Link from "@docusaurus/Link";

const AuthorName = ({ authors = [], className, withLink = true }) => {
  const renderName = (author) => {
    return (
      <span className={className} itemProp="name">
        {author.name}
      </span>
    );
  };
  return (
    <>
      {authors.map((author, idx) => (
        <div key={idx}>
          <div>
            {author.name && (
              <div>
                {idx !== 0 ? (idx !== authors.length - 1 ? "," : "and") : ""}
                {withLink ? (
                  <Link href={author.url} itemProp="url">
                    {renderName(author)}
                  </Link>
                ) : (
                  renderName(author)
                )}
              </div>
            )}
          </div>
        </div>
      ))}
    </>
  );
};

export default AuthorName;

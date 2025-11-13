import React from "react";
import Link from "@docusaurus/Link";

import styles from "@site/src/components/HomepageFeatures/styles.module.css";

const FeaturesBox = ({ data }) => {
  const Icon = data.icon;
  const { title, description, link } = data;
  return (
    <div className={styles.boxWrapper}>
      <Link to={link}>
        {data.icon && <Icon />}
        <h4 className={styles.featuresTitle}>{title}</h4>
        <p className={styles.featuresSubTitle}>{description}</p>
      </Link>
    </div>
  );
};
export default FeaturesBox;

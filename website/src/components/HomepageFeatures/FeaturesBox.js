import React from "react";
import styles from "@site/src/components/HomepageFeatures/styles.module.css";

const FeaturesBox = ({ data }) => {
  const Icon = data.icon;
  const { title, description } = data;
  return (
    <div className={styles.boxWrapper}>
      <div className={styles.iconWrapper}>{data.icon && <Icon />}</div>
      <h4 className={styles.featuresTitle}>{title}</h4>
      <p>{description}</p>
    </div>
  );
};
export default FeaturesBox;

import React from "react";
import styles from "@site/src/components/Title/styles.module.css";
import Heading from "@theme/Heading";
const Title = ({ primaryText, secondaryText, id }) => {
  return (
    <Heading as="h2" className={styles.titleWrapper} id={id}>
      <span className={styles.primaryTxt}>{primaryText}</span>&nbsp;
      <span className={styles.secondaryTxt}>{secondaryText}</span>
    </Heading>
  );
};

export default Title;

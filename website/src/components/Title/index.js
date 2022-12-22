import React from "react";
import styles from "@site/src/components/Title/styles.module.css";
import Heading from "@theme/Heading";
const AnchoredH2 = Heading("h2");
const Title = ({ primaryText, secondaryText, id }) => {
  return (
    <AnchoredH2 className={styles.titleWrapper} id={id}>
      <span className={styles.primaryTxt}>{primaryText}</span>&nbsp;
      <span className={styles.secondaryTxt}>{secondaryText}</span>
    </AnchoredH2>
  );
};

export default Title;

import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import { TypeAnimation } from 'react-type-animation';

import styles from "./styles.module.css";

const FeatureRender = () => {
  const { siteConfig } = useDocusaurusContext();
  const {
    taglineConfig: { prefix, suffix, content },
  } = siteConfig.customFields;
  return (
    <div className={styles.headlineWrapper}>
      <div>{prefix}</div>&nbsp;
      <TypeAnimation
          sequence={[
            content[0],
            1000,
            content[1],
            1000,
            content[2],
            1000
          ]}
          wrapper="div"
          cursor={true}
          repeat={Infinity}
          className={styles.typingText}
      />
      &nbsp;<div>{suffix}</div>
    </div>
  );
};

export default FeatureRender;

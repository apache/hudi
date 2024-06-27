import React from "react";
import clsx from "clsx";
import styles from "./styles.module.css";
import Link from "@docusaurus/Link";

const LinkButton = ({ type = "primary", className, ...res }) => {
  return (
    <Link
      className={clsx(styles.button, {
        [styles.primary]: type === "primary",
        [styles.secondary]: type === "secondary",
        [className]: className,
      })}
      {...res}
    />
  );
};
export default LinkButton;

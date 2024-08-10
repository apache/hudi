import React from 'react';
import Logo from '@theme/Logo';
import styles from './styles.module.css';
import clsx from 'clsx';

export default function NavbarLogo() {
  return (
    <Logo
      className="navbar__brand"
      imageClassName={clsx("navbar__logo", styles.navbarLogo)}
      titleClassName="navbar__title text--truncate"
    />
  );
}

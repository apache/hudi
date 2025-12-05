import React from 'react';
import clsx from 'clsx';
import {ThemeClassNames} from '@docusaurus/theme-common';
import styles from './styles.module.css'

export default function FooterLayout({style, links, logo, copyright}) {
  return (
    <footer
      className={clsx(
        ThemeClassNames.layout.footer.container,
        'footer',
        styles.footerCustom,           // module class
        {'footer--dark': style === 'dark'}
      )}
    >
      <div className={clsx('container', 'container-fluid', styles.container)}>
        <div className={styles.brand}>{logo}</div>
        <div className={styles.links}>{links}</div>
        {copyright && (
          <div className={styles.legal}>{copyright}</div>
        )}
      </div>
    </footer>
  );
}

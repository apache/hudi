/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import { useThemeConfig } from "@docusaurus/theme-common";
import useBaseUrl from "@docusaurus/useBaseUrl";
import isInternalUrl from "@docusaurus/isInternalUrl";
import styles from "./styles.module.css";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import ThemedImage from "@theme/ThemedImage";
import IconExternalLink from "@theme/IconExternalLink";
import LinkIcon from "./Icons/arrow _link.svg";

function FooterLink({ to, href, label, prependBaseUrlToHref, ...props }) {
  const toUrl = useBaseUrl(to);
  const normalizedHref = useBaseUrl(href, {
    forcePrependBaseUrl: true,
  });
  return (
    <Link
      className="footer__link-item"
      {...(href
        ? {
            href: prependBaseUrlToHref ? normalizedHref : href,
          }
        : {
            to: toUrl,
          })}
      {...props}
    >
      {href && !isInternalUrl(href) ? (
        <span className={styles.footerLinksIcon}>
          {label}
          <LinkIcon />
        </span>
      ) : (
        label
      )}
    </Link>
  );
}

function FooterLogo({ sources, alt, width, height }) {
  return (
    <ThemedImage
      className="footer__logo"
      alt={alt}
      sources={sources}
      width={width}
      height={height}
    />
  );
}

function MultiColumnLinks({ links }) {
  return (
    <>
      {links.map((linkItem, i) => (
        <div key={i} className="col footer__col">
          <div className="footer__title">{linkItem.title}</div>
          <ul className="footer__items">
            {linkItem.items.map((item, key) =>
              item.html ? (
                <li
                  key={key}
                  className="footer__item" // Developer provided the HTML, so assume it's safe.
                  // eslint-disable-next-line react/no-danger
                  dangerouslySetInnerHTML={{
                    __html: item.html,
                  }}
                />
              ) : (
                <li key={item.href || item.to} className="footer__item">
                  <FooterLink {...item} />
                </li>
              )
            )}
          </ul>
        </div>
      ))}
    </>
  );
}

function SimpleLinks({ links }) {
  return (
    <div className="footer__links">
      {links.map((item, key) => (
        <>
          {item.html ? (
            <span
              key={key}
              className="footer__link-item" // Developer provided the HTML, so assume it's safe.
              // eslint-disable-next-line react/no-danger
              dangerouslySetInnerHTML={{
                __html: item.html,
              }}
            />
          ) : (
            <FooterLink {...item} />
          )}
          {links.length !== key + 1 && (
            <span className="footer__link-separator">·</span>
          )}
        </>
      ))}
    </div>
  );
}

function isMultiColumnFooterLinks(links) {
  return "title" in links[0];
}

function Footer() {
  const { footer } = useThemeConfig();
  const {
    siteConfig: { customFields },
  } = useDocusaurusContext();
  const { copyright, links = [], logo = {} } = footer || {};
  const sources = {
    light: useBaseUrl(logo.src),
    dark: useBaseUrl(logo.srcDark || logo.src),
  };

  if (!footer) {
    return null;
  }

  return (
    <footer
      className={clsx("footer", {
        "footer--dark": footer.style === "dark",
      })}
    >
      <div className="container-fluid">
        <div className={styles.footerWrapper}>
          {logo && (logo.src || logo.srcDark) && (
            <div
              className={clsx("margin-bottom--sm", styles.footerLogoWrapper)}
            >
              {logo.href ? (
                <Link href={logo.href} className={styles.footerLogoLink}>
                  <FooterLogo
                    alt={logo.alt}
                    sources={sources}
                    width={logo.width}
                    height={logo.height}
                  />
                </Link>
              ) : (
                <FooterLogo alt={logo.alt} sources={sources} />
              )}
              <div className={styles.footerSubtitle}>
                {customFields?.copyrightText}
              </div>
            </div>
          )}
          {links &&
            links.length > 0 &&
            (isMultiColumnFooterLinks(links) ? (
              <div className={clsx("row footer__links", styles.footerLinks)}>
                <MultiColumnLinks links={links} />
              </div>
            ) : (
              <div className="footer__links text--center">
                <SimpleLinks links={links} />
              </div>
            ))}
        </div>
        {copyright && (
          <div className="footer__bottom text--center">
            {copyright ? (
              <div
                className={clsx("footer__copyright", styles.footerCopyright)} // Developer provided the HTML, so assume it's safe.
                // eslint-disable-next-line react/no-danger
                dangerouslySetInnerHTML={{
                  __html: copyright,
                }}
              />
            ) : null}
          </div>
        )}
      </div>
    </footer>
  );
}

export default React.memo(Footer);

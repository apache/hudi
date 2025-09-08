const { themes } = require("prism-react-renderer");
const versions = require("./versions.json");
const VersionsArchived = require("./versionsArchived.json");
const { slackUrl } = require('./constants');
const allDocHomesPaths = [
  "/docs/",
  "/docs/next/",
  ...versions.slice(1).map((version) => `/docs/${version}/`),
];

/** @type {import('@docusaurus/types').DocusaurusConfig} */

module.exports = {
  title: "Apache Hudi",
  tagline:
    "Hudi brings transactions, record-level updates/deletes and change streams to data lakes!",
  url: "https://hudi.apache.org",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",
  favicon: "/assets/images/favicon.ico",
  organizationName: "apache",
  projectName: "hudi",
  customFields: {
    copyrightText:
      "Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.",
    tagline:
      "Hudi brings transactions, record-level updates/deletes and change streams to data lakes!",
    taglineConfig: {
      prefix: "Hudi brings ",
      suffix: " to data lakes!",
      content: [
        "transactions",
        "row-level updates/deletes",
        "CDC and indexes"
      ],
    },
    slackUrl: slackUrl,
  },
  i18n: {
    defaultLocale: "en",
    locales: ["en", "cn"],
    localeConfigs: {
      en: {
        label: "English",
        direction: "ltr",
      },
      cn: {
        label: "Chinese",
        direction: "ltr",
      },
    },
  },
  plugins: [
    [
      "@docusaurus/plugin-content-blog",
      {
        id: "video-blog",
        path: "videoBlog",
        routeBasePath: "videos",
        blogSidebarCount: 0,
        onUntruncatedBlogPosts: "ignore",
        onInlineAuthors: "ignore",
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "contribute",
        path: "contribute",
        routeBasePath: "contribute",
        sidebarPath: require.resolve("./sidebarsContribute.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "community",
        path: "community",
        routeBasePath: "community",
        sidebarPath: require.resolve("./sidebarsCommunity.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "releases",
        path: "releases",
        routeBasePath: "releases",
        sidebarPath: require.resolve("./sidebarsReleases.js"),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "learn",
        path: "learn",
        routeBasePath: "learn",
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      "@docusaurus/plugin-client-redirects",
      {
        fromExtensions: ["html"],
        createRedirects: function (path) {
          // redirect to /docs from /docs/introduction,
          // as introduction has been made the home doc
          if (allDocHomesPaths.includes(path)) {
            return [`${path}/quick-start-guide`];
          }
        },
        redirects: [
          {
            from: [
              "/docs/contribute",
              "/docs/next/contribute",
              "/contribute/get-involved",
            ],
            to: "/community/get-involved",
          },
          {
            from: ["/contribute/team"],
            to: "/community/team",
          },
          {
            from: ["/docs/next/hoodie_cleaner"],
            to: "/docs/next/cleaning",
          },
          {
            from: ["/docs/next/indexing"],
            to: "/docs/next/indexes",
          },
          {
            from: ["/docs/releases", "/docs/next/releases"],
            to: "/releases/release-1.0.2",
          },
          {
            from: ["/releases"],
            to: "/releases/release-1.0.2",
          },
        ],
      },
    ],
  ],
  themeConfig: {
    metadata: [
      {
        name: "keywords",
        content:
          "apache hudi, data lake, lakehouse, big data, apache spark, apache flink, presto, trino, analytics, data engineering",
      },
    ],
    algolia: {
      apiKey: "e300f1558b703c001c515c0e7f8e0908",
      indexName: "apache_hudi",
      contextualSearch: true,
      appId: "BH4D9OD16A",
    },
    navbar: {
      logo: {
        alt: "Apache Hudi",
        src: "assets/images/hudi.png",
      },
      items: [
        {
          label: "Docs",
          to: "/docs/overview",
        },
        {
          label: "Learn",
          position: "left",
          items: [
            {
              label: "Tutorial Series",
              to: "/learn/tutorial-series",
            },
            {
              label: "Talks",
              to: "talks",
            },
            {
              label: "Blog",
              to: "/blog",
            },
            {
              label: "Video Guides",
              to: "videos",
            },
            {
              label: "FAQ",
              href: "/docs/faq",
            },
            {
              label: "Tech Specs",
              href: "/tech-specs",
            },
            {
              label: "Tech Specs 1.0",
              href: "/tech-specs-1point0",
            },
            {
              label: "Technical Wiki",
              href: "https://cwiki.apache.org/confluence/display/HUDI",
            },
          ],
        },
        {
          label: "Contribute",
          position: "left",
          items: [
            {
              label: "Developer Sync Call",
              to: "/contribute/developer-sync-call",
            },
            {
              label: "How to Contribute",
              to: "/contribute/how-to-contribute",
            },
            {
              label: "Developer Setup",
              to: "/contribute/developer-setup",
            },
            {
              label: "RFC Process",
              to: "/contribute/rfc-process",
            },
            {
              label: "Report Security Issues",
              to: "/contribute/report-security-issues",
            },
            {
              label: "Report Issues",
              href: "https://issues.apache.org/jira/projects/HUDI/summary",
            },
          ],
        },
        {
          label: "Community",
          position: "left",
          items: [
            {
              label: "Get Involved",
              to: "/community/get-involved",
            },
            {
              label: "Community Syncs",
              to: "/community/syncs",
            },
            {
              label: "Office Hours",
              to: "/community/office_hours",
            },
            {
              label: "Team",
              to: "/community/team",
            },
            {
              label: 'Join Our Slack Space',
              href: slackUrl,
            },
          ],
        },
        { to: "/ecosystem", label: "Ecosystem", position: "left" },
        { to: "/powered-by", label: "Who's Using", position: "left" },
        { to: "/roadmap", label: "Roadmap", position: "left" },
        { to: "/releases/download", label: "Download", position: "left" },
        // right
        {
          type: "docsVersionDropdown",
          position: "right",
          dropdownActiveClassDisabled: true,
          dropdownItemsAfter: [
            ...Object.entries(VersionsArchived).map(
              ([versionName, versionUrl]) => ({
                label: versionName,
                href: versionUrl,
              })
            ),
          ],
        },
        {
          type: "localeDropdown",
          position: "right",
        },
        {
          href: "https://github.com/apache/hudi",
          position: "right",
          className: "header-github-link",
          "aria-label": "GitHub repository",
        },
        {
          href: "https://twitter.com/ApacheHudi",
          position: "right",
          className: "header-twitter-link",
          "aria-label": "Hudi Twitter Handle",
        },
        {
          href: slackUrl,
          position: "right",
          className: "header-slack-link",
          "aria-label": "Hudi Slack Channel",
        },
        {
          href: "https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw",
          position: "right",
          className: "header-youtube-link",
          "aria-label": "Hudi YouTube Channel",
        },
        {
          href: "https://www.linkedin.com/company/apache-hudi/?viewAsMember=true",
          position: "right",
          className: "header-linkedin-link",
          "aria-label": "Hudi Linkedin Page",
        },
      ],
    },
    footer: {
      style: "light",
      links: [
        {
          title: "About",
          items: [
            {
              label: "Our Vision",
              to: "/blog/2021/07/21/streaming-data-lake-platform",
            },
            {
              label: "Concepts",
              to: "/docs/concepts",
            },
            {
              label: "Team",
              to: "/community/team",
            },
            {
              label: "Releases",
              to: "/releases/release-1.0.2",
            },
            {
              label: "Download",
              to: "/releases/download",
            },
            {
              label: "Who's Using",
              to: "powered-by",
            },
          ],
        },
        {
          title: "Learn",
          items: [
            {
              label: "Quick Start",
              to: "/docs/quick-start-guide",
            },
            {
              label: "Tutorial Series",
              to: "/learn/tutorial-series",
            },
            {
              label: "Blog",
              to: "/blog",
            },
            {
              label: "Talks",
              to: "talks",
            },
            {
              label: "Video Guides",
              to: "videos",
            },
            {
              label: "FAQ",
              href: "/docs/faq",
            },
          ],
        },
        {
          title: "Hudi On Cloud",
          items: [
            {
              label: "AWS",
              to: "/docs/s3_hoodie",
            },
            {
              label: "Google Cloud",
              to: "/docs/gcs_hoodie",
            },
            {
              label: "Alibaba Cloud",
              to: "/docs/oss_hoodie",
            },
            {
              label: "Microsoft Azure",
              to: "/docs/azure_hoodie",
            },
            {
              label: "Tencent Cloud",
              to: "/docs/cos_hoodie",
            },
            {
              label: "IBM Cloud",
              to: "/docs/ibm_cos_hoodie",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Get Involved",
              to: "/community/get-involved",
            },
            {
              label: "Slack",
              href: slackUrl,
            },
            {
              label: "GitHub",
              href: "https://github.com/apache/hudi",
            },
            {
              label: "Twitter",
              href: "https://twitter.com/ApacheHudi",
            },
            {
              label: "YouTube",
              href: "https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw",
            },
            {
              label: "Linkedin",
              href: "https://www.linkedin.com/company/apache-hudi/?viewAsMember=true",
            },
            {
              label: "Mailing List",
              to: "mailto:dev-subscribe@hudi.apache.org?Subject=SubscribeToHudi",
            },
          ],
        },
        {
          title: "Apache",
          items: [
            {
              label: "Events",
              to: "https://www.apache.org/events/current-event",
            },
            {
              label: "Thanks",
              to: "https://www.apache.org/foundation/thanks.html",
            },
            {
              label: "License",
              to: "https://www.apache.org/licenses",
            },
            {
              label: "Security",
              to: "https://www.apache.org/security",
            },
            {
              label: "Privacy",
              to: "https://privacy.apache.org/policies/privacy-policy-public.html",
            },
            {
              label: "Telemetry",
              to: "/asf/telemetry",
            },
            {
              label: "Sponsorship",
              to: "https://www.apache.org/foundation/sponsorship.html",
            },
            {
              label: "Foundation",
              to: "https://www.apache.org",
            },
          ],
        },
      ],
      logo: {
        alt: "Apache Hudi™",
        src: "/assets/images/logo-big.png",
        href: "https://hudi.apache.org/",
      },
      copyright:
        'Copyright © 2021 <a href="https://apache.org">The Apache Software Foundation</a>, Licensed under the <a href="https://www.apache.org/licenses/LICENSE-2.0"> Apache License, Version 2.0</a>. <br />Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=8f594acf-9b77-44fb-9475-3e82ead1910c" /><img referrerpolicy="no-referrer-when-downgrade" src="https://analytics.apache.org/matomo.php?idsite=47&amp;rec=1" />',
    },
    prism: {
      theme: themes.dracula,
      additionalLanguages: ["java", "scala"],
      prismPath: require.resolve("./src/theme/prism-include-languages.js"),
    },
    announcementBar: {
      id: "announcementBar-2",
      content:
        "⭐️ If you like <b>Apache Hudi</b>, give it a star on <a target=\"_blank\" rel=\"noopener noreferrer\" href=\"https://github.com/apache/hudi\"><b>GitHub!<svg xmlns='http://www.w3.org/2000/svg\\' width='16' height='16' fill='currentColor' class='bi bi-github' viewBox='0 -2 16 16'><path d='M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z'/></svg></b></a> ⭐",
      isCloseable: false,
    },
    colorMode: {
      defaultMode: "light",
      disableSwitch: true,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          // Please change this to your repo.
          editUrl: ({ version, versionDocsDirPath, docPath, locale }) => {
            if (locale != this.defaultLocale) {
              return `https://github.com/apache/hudi/tree/asf-site/website/${versionDocsDirPath}/${docPath}`;
            } else {
              return `https://github.com/apache/hudi/tree/asf-site/website/i18n/${locale}/docusaurus-plugin-content-${versionDocsDirPath}/${version}/${docPath}`;
            }
          },
          includeCurrentVersion: true,
          versions: {
            current: {
              label: "Current",
              path: "next",
              banner: "unreleased",
            },
            "1.0.2": {
              label: "1.0.2",
              path: "",
            },
          },
        },
        blog: {
          editUrl: "https://github.com/apache/hudi/edit/asf-site/website/blog/",
          blogTitle: "Blogs List Page",
          blogSidebarCount: 0,
          blogSidebarTitle: "Recent posts",
          /**
           * URL route for the blog section of your site.
           * *DO NOT* include a trailing slash.
           */
          routeBasePath: "blog",
          include: ["*.md", "*.mdx"],
          postsPerPage: 12,
          /**
           * Theme components used by the blog pages.
           */
          blogListComponent: "@theme/BlogListPage",
          blogPostComponent: "@theme/BlogPostPage",
          blogTagsListComponent: "@theme/BlogTagsListPage",
          blogTagsPostsComponent: "@theme/BlogTagsPostsPage",
          feedOptions: {
            type: "all",
            title: "Apache Hudi: User-Facing Analytics",
          },
          showReadingTime: true,
          onUntruncatedBlogPosts: "ignore",
          onInlineAuthors: "ignore",
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],
  scripts: [],
  stylesheets: [
    "https://fonts.googleapis.com/css?family=Comfortaa|Ubuntu|Roboto|Source+Code+Pro",
    "https://at-ui.github.io/feather-font/css/iconfont.css",
  ],
};

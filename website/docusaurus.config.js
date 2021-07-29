const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const versions = require('./versions.json');
const VersionsArchived = require('./versionsArchived.json');
const allDocHomesPaths = [
  '/docs/',
  '/docs/next/',
  ...versions.slice(1).map((version) => `/docs/${version}/`),
];

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Welcome to Apache Hudi!',
  tagline: 'Apache Hudi ingests & manages storage of large analytical datasets over DFS (hdfs or cloud stores).',
  url: 'https://hudi.apache.org',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'assets/image/favicon.ico',
  organizationName: 'apache', // Usually your GitHub org/user name.
  projectName: 'hudi', // Usually your repo name.
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'cn'],
    localeConfigs: {
      en: {
        label: 'English',
        direction: 'ltr',
      },
      cn: {
        label: 'Chinese',
        direction: 'ltr',
      },
    },
  },
  plugins: [
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'contribute',
        path: 'contribute',
        routeBasePath: 'contribute',
        sidebarPath: require.resolve('./sidebarsContribute.js'),
        showLastUpdateAuthor: true,
        showLastUpdateTime: true,
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'releases',
        path: 'releases',
        routeBasePath: 'releases',
        sidebarPath: require.resolve('./sidebarsReleases.js'),
        showLastUpdateAuthor: true,
        showLastUpdateTime: true,
      },
    ],
    [
      '@docusaurus/plugin-client-redirects',
      {
        fromExtensions: ['html'],
        createRedirects: function (path) {
          // redirect to /docs from /docs/introduction,
          // as introduction has been made the home doc
          if (allDocHomesPaths.includes(path)) {
            return [`${path}/quick-start-guide`];
          }
        },
        redirects: [
          {
            from: ['/docs/contribute', '/docs/next/contribute'],
            to: '/contribute/get-involved',
          },
          {
            from: ['/docs/releases', '/docs/next/releases'],
            to: '/releases/release-0.8.0',
          },
          {
            from: ['/releases'],
            to: '/releases/release-0.8.0',
          },
        ],
      },
    ],
  ],
  themeConfig: {
    navbar: {
      logo: {
        alt: 'Apache Hudi',
        src: 'assets/images/hudi.png',
      },
      items: [
        {
          label: 'Docs',
          to: '/docs/quick-start-guide',
        },
        {
          label: 'Learn',
          position: 'left',
          items: [
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'Talks & Articles',
              to: 'talks-articles',
            },
            {
              label: 'FAQ',
              href: 'https://cwiki.apache.org/confluence/display/HUDI/FAQ',
            },
            {
              label: 'Technical Wiki',
              href: 'https://cwiki.apache.org/confluence/display/HUDI',
            }
          ],
        },
        {
          label: 'Contribute',
          position: 'left',
          items: [
            {
              label: 'Get Involved',
              to: '/contribute/get-involved',
            },
            {
              label: 'Team',
              to: '/contribute/team',
            },
            {
              label: 'How to Contribute',
              to: '/contribute/how-to-contribute',
            },
            {
              label: 'Developer Setup',
              to: '/contribute/developer-setup',
            },
            {
              label: 'Report Security Issues',
              to: '/contribute/report-security-issues',
            },
            {
              label: 'Report Issues',
              href: 'https://issues.apache.org/jira/projects/HUDI/summary',
            }
          ],
        },
        {to: '/powered-by', label: "Who's Using", position: 'left'},
        {to: '/releases/download', label: 'Download', position: 'left'},
        // right
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownActiveClassDisabled: true,
          dropdownItemsAfter: [
            ...Object.entries(VersionsArchived).map(
                ([versionName, versionUrl]) => ({
                  label: versionName,
                  href: versionUrl,
                }),
            )
          ],
        },
        {
          type: 'localeDropdown',
          position: 'right',
        },
        {
          href: 'https://github.com/apache/hudi',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
        {
          href: 'https://twitter.com/ApacheHudi',
          position: 'right',
          className: 'header-twitter-link',
          'aria-label': 'Hudi Twitter Handle',
        },
        {
          href: 'https://join.slack.com/t/apache-hudi/shared_invite/enQtODYyNDAxNzc5MTg2LTE5OTBlYmVhYjM0N2ZhOTJjOWM4YzBmMWU2MjZjMGE4NDc5ZDFiOGQ2N2VkYTVkNzU3ZDQ4OTI1NmFmYWQ0NzE',
          position: 'right',
          className: 'header-slack-link',
          'aria-label': 'Hudi Slack Channel',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'About',
          items: [
            {
              label: 'Concepts',
              to: '/docs/concepts',
            },
            {
              label: 'Roadmap',
              to: '/roadmap',
            },
            {
              label: 'Team',
              to: '/contribute/team',
            },
            {
              label: 'Releases',
              to: '/releases/release-0.8.0',
            },
            {
              label: 'Download',
              to: '/releases/download',
            },
            {
              label: 'Who\'s Using',
              to: 'powered-by',
            },
          ],
        },
        {
          title: 'Learn',
          items: [
            {
              label: 'Quick Start',
              to: '/docs/quick-start-guide',
            },
            {
              label: 'Docker Demo',
              to: '/docs/docker_demo',
            },
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'Talks & Articles',
              to: 'talks-articles',
            },
            {
              label: 'FAQ',
              href: 'https://cwiki.apache.org/confluence/display/HUDI/FAQ',
            },
            {
              label: 'Technical Wiki',
              href: 'https://cwiki.apache.org/confluence/display/HUDI',
            }
          ],
        },
        {
          title: 'Hudi On Cloud',
          items: [
            {
              label: 'AWS',
              to: '/docs/s3_hoodie',
            },
            {
              label: 'Google Cloud',
              to: '/docs/gcs_hoodie',
            },
            {
              label: 'Alibaba Cloud',
              to: '/docs/oss_hoodie',
            },
            {
              label: 'Microsoft Azure',
              to: '/docs/azure_hoodie',
            },
            {
              label: 'Tencent Cloud',
              to: '/docs/cos_hoodie',
            },
            {
              label: 'IBM Cloud',
              to: '/docs/ibm_cos_hoodie',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Get Involved',
              to: '/contribute/get-involved'
            },
            {
              label: 'Slack',
              href: 'https://join.slack.com/t/apache-hudi/shared_invite/enQtODYyNDAxNzc5MTg2LTE5OTBlYmVhYjM0N2ZhOTJjOWM4YzBmMWU2MjZjMGE4NDc5ZDFiOGQ2N2VkYTVkNzU3ZDQ4OTI1NmFmYWQ0NzE',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/apache/hudi',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/ApacheHudi',
            },
            {
              label: 'Mailing List',
              to: 'mailto:dev-subscribe@hudi.apache.org?Subject=SubscribeToHudi',
            },
          ],
        },
        {
          title: 'Apache',
          items: [
            {
              label: 'Events',
              to: 'https://www.apache.org/events/current-event',
            },
            {
              label: 'Thanks',
              to: 'https://www.apache.org/foundation/thanks.html',
            },
            {
              label: 'License',
              to: 'https://www.apache.org/licenses',
            },
            {
              label: 'Security',
              to: 'https://www.apache.org/security',
            },
            {
              label: 'Sponsorship',
              to: 'https://www.apache.org/foundation/sponsorship.html',
            },
            {
              label: 'Foundation',
              to: 'https://www.apache.org',
            },
          ],
        },
      ],
      logo: {
        alt: 'Apache Hudi™',
        src: '/assets/images/logo-big.png',
        href: 'https://hudi.apache.org/',
      },
      copyright: 'Copyright © 2021 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. <br />Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.',
    },
    prism: {
      theme: darkCodeTheme,
      additionalLanguages: ['java', 'scala'],
      prismPath: require.resolve('./src/theme/prism-include-languages.js'),
    },
    announcementBar: {
      id: 'announcementBar-1', // Increment on change
      content:
          '⭐️ If you like Apache Hudi, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/apache/hudi">GitHub</a>! ⭐',
    },
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          editUrl:
            'https://github.com/apache/hudi/edit/asf-site/website/docs/',
        },
        blog: {
          editUrl:
            'https://github.com/apache/hudi/edit/asf-site/website/blog/',
          blogTitle: 'Blog - Apache Hudi: User-Facing Analytics',
          blogSidebarCount: 10,
          blogSidebarTitle: 'Recent posts',
          /**
           * URL route for the blog section of your site.
           * *DO NOT* include a trailing slash.
           */
          routeBasePath: 'blog',
          include: ['*.md', '*.mdx'],
          postsPerPage: 10,
          /**
           * Theme components used by the blog pages.
           */
          blogListComponent: '@theme/BlogListPage',
          blogPostComponent: '@theme/BlogPostPage',
          blogTagsListComponent: '@theme/BlogTagsListPage',
          blogTagsPostsComponent: '@theme/BlogTagsPostsPage',
          feedOptions: {
            type: "all",
            title: 'Apache Hudi: User-Facing Analytics',
          },
          showReadingTime: true,
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
  scripts: [],
  stylesheets: [
    'https://fonts.googleapis.com/css?family=Comfortaa|Ubuntu|Roboto|Source+Code+Pro',
    'https://at-ui.github.io/feather-font/css/iconfont.css',
  ],
};

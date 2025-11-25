import React from 'react';
import styles from './styles.module.css';
import Title from "@site/src/components/Title";
import { originalSlackUrl } from '../../../constants';

const GetInvolvedGrid = () => {
  const data = [
    {
      title: 'For development discussions',
      content: (
        <>
          <a href="https://github.com/apache/hudi/discussions">Github Discussions</a> or Dev Mailing list (
          <a href="mailto:dev-subscribe@hudi.apache.org">Subscribe</a>,{' '}
          <a href="mailto:dev-unsubscribe@hudi.apache.org">Unsubscribe</a>,{' '}
          <a href="https://lists.apache.org/list?dev@hudi.apache.org">Archives</a>).
          <br />
          Empty email works for subscribe/unsubscribe.
        </>
      ),
    },
    {
      title: 'For any general questions, user support',
      content: (
        <>
          <a href="https://github.com/apache/hudi/discussions">Github Discussions</a> or Users Mailing list (
          <a href="mailto:users-subscribe@hudi.apache.org">Subscribe</a>,{' '}
          <a href="mailto:users-unsubscribe@hudi.apache.org">Unsubscribe</a>,{' '}
          <a href="https://lists.apache.org/list?users@hudi.apache.org">Archives</a>).
          <br />
          Empty email works for subscribe/unsubscribe.
        </>
      ),
    },
    {
      title: 'For reporting bugs or known issues',
      content: (
        <>
          Use Github <a href="https://github.com/apache/hudi/issues">Issues</a>, please read guidelines{' '}
          <a href="/contribute/how-to-contribute#filing-issues">here</a>.
        </>
      ),
    },
    {
      title: 'For quick pings & 1-1 chats',
      content: (
        <>
          Join our <a href={originalSlackUrl}>Slack space</a>. Or drop an email to{' '}
          <a href="mailto:dev@hudi.apache.org">dev@hudi.apache.org</a>.
        </>
      ),
    },
    {
      title: 'For proposing large features, changes',
      content: (
        <>
          Start a RFC. Instructions <a href="/contribute/rfc-process">here</a>.
        </>
      ),
    },
    {
      title: 'Join sync-up meetings',
      content: (
        <>
          <a href="/community/syncs">Community sync</a> and <a href="/contribute/developer-sync-call">Dev Sync</a>.
        </>
      ),
    },
    {
      title: 'For stream of commits, pull requests etc',
      content: (
        <>
          Commits Mailing list (
          <a href="mailto:commits-subscribe@hudi.apache.org">Subscribe</a>,{' '}
          <a href="mailto:commits-unsubscribe@hudi.apache.org">Unsubscribe</a>,{' '}
          <a href="https://lists.apache.org/list?commits@hudi.apache.org">Archives</a>).
        </>
      ),
    },
  ];

  return (
    <div>
      <div className={styles.getInvloved}>
        <Title primaryText="Get Involved"/>
        <div className={styles.subTitle}>
          There are several ways to get in touch with the Hudi community.
        </div>
      </div>
      <div className={styles.container}>
        {data.map((item, idx) => (
          <div key={idx} className={styles.card}>
            <h3 className={styles.cardTitle}>{item.title}</h3>
            <p className={styles.cardDesc}>{item.content}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

export default GetInvolvedGrid

import React, { useEffect } from 'react';
import { Redirect } from '@docusaurus/router';

export default function SlackRedirect() {
  useEffect(() => {
    window.location.href = "https://join.slack.com/t/apache-hudi/shared_invite/zt-33fabmxb7-Q7QSUtNOHYCwUdYM8LbauA";
  }, []);
  return <p>Redirecting to Slack...</p>;
}
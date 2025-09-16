import React, { useEffect } from 'react';
import { Redirect } from '@docusaurus/router';
import { slackUrl } from '../../constants';

export default function SlackRedirect() {
  useEffect(() => {
    window.location.href =  slackUrl;
  }, []);
  return <p>Redirecting to Slack...</p>;
}
import React, { useEffect } from 'react';
import { originalSlackUrl } from '../../constants';

export default function SlackRedirect() {
  useEffect(() => {
    window.location.href = originalSlackUrl ;
  }, []);
  return <p>Redirecting to Slack...</p>;
}

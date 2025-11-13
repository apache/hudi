import React, {useState} from 'react';
import Link from '@docusaurus/Link';
import {
  useVersions,
  useActiveDocContext,
  useDocsPreferredVersion,
} from '@docusaurus/plugin-content-docs/client';
import styles from './styles.module.css';
import clsx from 'clsx';
import {useDocsSidebar} from '@docusaurus/plugin-content-docs/client';

function getTargetPath(version, ctx) {
  return (ctx.alternateDocVersions?.[version.name] ?? version.mainDoc).path;
}

const SidebarVersions = ({docsPluginId = 'default'}) =>  {
  const sidebar = useDocsSidebar();
  const [open, setOpen] = useState(false);
  const versions = useVersions(docsPluginId);
  const ctx = useActiveDocContext(docsPluginId);
  const {savePreferredVersionName} = useDocsPreferredVersion(docsPluginId);

  if (!versions?.length || sidebar.name !== 'docs') return null;

  const sorted = [...versions].sort((a, b) => (a.label < b.label ? 1 : -1));
  const headerLabel = `Version ${ctx.activeVersion?.label ?? sorted[0].label}`;

  return (
    <div className={styles.versions}>
      <hr className={styles.divider}/>
      <button className={styles.header} onClick={() => setOpen(o => !o)}>
        {headerLabel}
        <span className={clsx(styles.caret, open && styles.caretOpen)}/>
      </button>

      {open && (
        <ul className={styles.list}>
          {sorted.map(v => {
            const to = getTargetPath(v, ctx);
            const isActive = v.name === ctx.activeVersion?.name;
            return (
              <li key={v.name} className="menu__list-item">
                <Link
                  to={to}
                  className={clsx(styles.link, isActive && styles.linkActive)}
                  onClick={() => savePreferredVersionName(v.name)}
                >
                  {v.label}
                </Link>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

export default SidebarVersions

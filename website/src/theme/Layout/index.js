import React from 'react';
import clsx from 'clsx';
import ErrorBoundary from '@docusaurus/ErrorBoundary';
import { ThemeClassNames } from '@docusaurus/theme-common';
import {useKeyboardNavigation} from '@docusaurus/theme-common/internal';
import SkipToContent from '@theme/SkipToContent';
import AnnouncementBar from '@theme/AnnouncementBar';
import Navbar from '@theme/Navbar';
import Footer from '@theme/Footer';
import LayoutProviders from '@theme/Layout/Provider';
import ErrorPageContent from '@theme/ErrorPageContent';


export default function Layout(props) {
    const {children, noFooter, wrapperClassName, pageClassName} = props;
    useKeyboardNavigation();
    return (
        <LayoutProviders {...props}>

            <SkipToContent />

            <AnnouncementBar />

            <Navbar />

            <div
                className={clsx(
                    ThemeClassNames.wrapper.main,
                    wrapperClassName,
                    pageClassName,
                )}>
                <ErrorBoundary fallback={ErrorPageContent}>{children}</ErrorBoundary>
            </div>

            {!noFooter && <Footer />}
        </LayoutProviders>
    );
}

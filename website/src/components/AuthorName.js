import React from "react";
import Link from '@docusaurus/Link';

const AuthorName = ({authors = [], className}) => {
    return (
        <>
            {authors.map((author, idx) => (
                <div key={idx}>
                    <div>
                        {author.name && (
                            <div>
                                {idx !== 0 ? idx !== authors.length - 1 ? ',' : 'and' : ''}
                                <Link href={author.url} itemProp="url">
                                    <span className={className} itemProp="name">{author.name}</span>
                                </Link>
                            </div>
                            )
                        }
                    </div>
                </div>
            ))}
        </>
    )
}

export default AuthorName;

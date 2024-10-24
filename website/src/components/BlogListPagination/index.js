import { useState, useEffect } from "react";
import { useLocation } from "@docusaurus/router";
import styles from "@site/src/components/BlogListPagination/styles.module.css";

const BlogListPagination = ({ numOfPages, currentPage, handlePageChange }) => {
  const location = useLocation();

  const [arrOfCurrButtons, setArrOfCurrButtons] = useState([]);

  const numOfButtons = [];
  for (let i = 1; i <= numOfPages; i++) {
    numOfButtons.push(i);
  }

  const prevPageClick = () => {
    if (currentPage === 1) {
      handlePageChange(currentPage);
    } else {
      handlePageChange(currentPage - 1);
    }
  };

  const nextPageClick = () => {
    if (currentPage === numOfButtons.length) {
      handlePageChange(currentPage);
    } else {
      handlePageChange(currentPage + 1);
    }
  };

  useEffect(() => {
    let tempNumberOfButtons = [...arrOfCurrButtons];

    let dotsInitial = "...";
    let dotsLeft = "... ";
    let dotsRight = " ...";

    if (numOfButtons.length < 5) {
      tempNumberOfButtons = numOfButtons;
    } else if (currentPage >= 1 && currentPage <= 2) {
      tempNumberOfButtons = [1, 2, 3, dotsInitial, numOfButtons.length];
    } else if (currentPage === 3) {
      const sliced = numOfButtons.slice(0, 4);
      tempNumberOfButtons = [...sliced, dotsInitial, numOfButtons.length];
    } else if (currentPage > 3 && currentPage < numOfButtons.length - 2) {
      const sliced1 = numOfButtons.slice(currentPage - 2, currentPage);
      const sliced2 = numOfButtons.slice(currentPage, currentPage + 1);
      tempNumberOfButtons = [
        1,
        dotsLeft,
        ...sliced1,
        ...sliced2,
        dotsRight,
        numOfButtons.length,
      ];
    } else if (currentPage > numOfButtons.length - 3) {
      const sliced = numOfButtons.slice(numOfButtons.length - 4);
      tempNumberOfButtons = [1, dotsLeft, ...sliced];
    } else if (currentPage === dotsInitial) {
      handlePageChange(arrOfCurrButtons[arrOfCurrButtons.length - 3] + 1);
    } else if (currentPage === dotsRight) {
      handlePageChange(arrOfCurrButtons[3] + 2);
    } else if (currentPage === dotsLeft) {
      handlePageChange(arrOfCurrButtons[3] - 2);
    }

    setArrOfCurrButtons(tempNumberOfButtons);
  }, [currentPage, numOfPages]);

  return (
    <div className={styles.table_filter_info}>
      <div className={styles.dt_pagination}>
        <ul className={styles.dt_pagination_ul}>
          <li
            className={`${styles.dt_item} ${
              currentPage === 1 ? styles.disabled : ""
            }`}
          >
            <a className={styles.dt_link} onClick={prevPageClick}>
              Prev
            </a>
          </li>
          {arrOfCurrButtons.map((data, index) => {
            const dots = typeof data === "string" ? data?.includes("...") : "";
            return (
              <li
                key={index}
                className={`${styles.dt_item} ${
                  currentPage === data ? styles.active : ""
                } ${dots ? styles.dots : ""} `}
              >
                <a
                  className={styles.dt_link}
                  onClick={() => handlePageChange(data)}
                >
                  {data}
                </a>
              </li>
            );
          })}
          <li
            className={`${styles.dt_item} ${
              currentPage === numOfButtons.length ? styles.disabled : ""
            }`}
          >
            <a className={styles.dt_link} onClick={nextPageClick}>
              Next
            </a>
          </li>
        </ul>
      </div>
    </div>
  );
};

export default BlogListPagination;

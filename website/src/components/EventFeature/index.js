import React from "react";
import styles from "./styles.module.css";
import LinkButton from "@site/src/components/UI/LinkButton";
import Calendar from "./Icons/calendar.svg";
import Microphone from "./Icons/microphone.svg";

const Events = () => {
    return (
      <section className={styles.eventWrapper}>
        <div className="container">
          <div className={styles.eventContainer}>
            <p className={styles.upcomingEventTitle}>
              UPCOMING EVENT
            </p>
            <div className={styles.eventContent}>
              <div className={styles.textContainer}>
                <h1 className={styles.title}>Apache Hudi Meetup | ASIA (Chinese)</h1>
                <h3 className={styles.subTitle}>Next-Generation Lakehouse: The Intelligent Future Engine</h3>
                <p className={styles.timeText}>Oct 23 (Thursday) 14:00 - 17:30 (China Standard Time) | Beijing JD.com
                  Headquarters</p>
              </div>
              <div className={styles.rsvpTodayBtn}>
                <LinkButton class={styles.blogBtn} to="https://www.bagevent.com/event/9098955">
                  RSVP Today
                </LinkButton>
              </div>
            </div>
          </div>
        </div>
      </section>
    );
}
export default Events;

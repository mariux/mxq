LOCK TABLES mxq_job WRITE, mxq_group WRITE;
DELIMITER |
DROP TRIGGER IF EXISTS mxq_add_group|
CREATE TRIGGER mxq_add_group BEFORE INSERT ON mxq_group
    FOR EACH ROW BEGIN
        SET NEW.group_mtime = NOW();

        IF (NEW.group_jobs_running = 0 AND NEW.group_jobs_inq = 0) THEN
            SET NEW.group_date_end = NEW.group_mtime;
        ELSEIF  (NEW.group_jobs_running > 0 OR NEW.group_jobs_inq > 0) THEN
            SET NEW.group_date_end = 0;
        END IF;
    END;
|
DROP TRIGGER IF EXISTS mxq_update_group|
CREATE TRIGGER mxq_update_group BEFORE UPDATE ON mxq_group
    FOR EACH ROW BEGIN
        SET NEW.group_mtime = NOW();

        IF OLD.group_jobs_inq > 0 AND OLD.group_jobs_running = 0 THEN
            SET NEW.stats_wait_sec = OLD.stats_wait_sec + (UNIX_TIMESTAMP(NEW.group_mtime) - UNIX_TIMESTAMP(OLD.group_mtime));
        ELSEIF OLD.group_jobs_running > 0 THEN
            SET NEW.stats_run_sec = OLD.stats_run_sec + (UNIX_TIMESTAMP(NEW.group_mtime) - UNIX_TIMESTAMP(OLD.group_mtime));
        END IF;

        IF (NEW.group_jobs_running = 0 AND NEW.group_jobs_inq = 0) AND
           (OLD.group_jobs_running > 0 OR OLD.group_jobs_inq > 0) THEN
            SET NEW.group_date_end = NEW.group_mtime;
        ELSEIF  (OLD.group_jobs_running = 0 AND OLD.group_jobs_inq = 0) AND
                (NEW.group_jobs_running > 0 OR NEW.group_jobs_inq > 0) THEN
            SET NEW.stats_idle_sec = OLD.stats_idle_sec + (UNIX_TIMESTAMP(NEW.group_mtime) - UNIX_TIMESTAMP(OLD.group_date_end));
            SET NEW.group_date_end = 0;
        END IF;
    END;
|
DROP TRIGGER IF EXISTS mxq_add_job|
CREATE TRIGGER mxq_add_job AFTER INSERT ON mxq_job
    FOR EACH ROW BEGIN
        UPDATE mxq_group SET
            group_jobs     = group_jobs     + 1,
            group_jobs_inq = group_jobs_inq + 1
        WHERE group_id = NEW.group_id;
    END;
|
DROP TRIGGER IF EXISTS mxq_update_job|
CREATE TRIGGER mxq_update_job BEFORE UPDATE ON mxq_job
    FOR EACH ROW BEGIN
        IF NEW.job_status != OLD.job_status THEN

            -- ASSIGNED(100) -> LOADED(150)
            IF NEW.job_status = 150 AND OLD.job_status = 100  THEN

                IF NEW.daemon_id != 0 THEN
                    UPDATE mxq_daemon SET
                        mtime = NULL,
                        daemon_slots_running = daemon_slots_running + NEW.host_slots,
                        daemon_jobs_running  = daemon_jobs_running  + 1
                    WHERE daemon_id = NEW.daemon_id;
                END IF;

                UPDATE mxq_group SET
                    group_jobs_inq      = group_jobs_inq      - 1,
                    group_jobs_running  = group_jobs_running  + 1,
                    group_slots_running = group_slots_running + NEW.host_slots
                WHERE group_id = NEW.group_id;

            -- LOADED(150) -> RUNNING(200)
            ELSEIF NEW.job_status = 200 AND OLD.job_status = 150 THEN

                UPDATE mxq_group SET
                   group_slots_running=group_slots_running-OLD.host_slots+NEW.host_slots,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;

                IF OLD.host_slots != NEW.host_slots THEN
                    IF NEW.daemon_id != 0 THEN
                        UPDATE mxq_daemon SET
                            mtime = NULL,
                            daemon_slots_running = daemon_slots_running + NEW.host_slots - OLD.host_slots
                        WHERE daemon_id = NEW.daemon_id;
                    END IF;
                END IF;

            -- LOADED(150) | RUNNING(200) | UNKNOWN_RUN(250) | EXTRUNNING(300) | STOPPED(350) | KILLING(399) -> KILLED(400) | FAILED(750)
            ELSEIF NEW.job_status IN (400, 750) AND OLD.job_status IN (150, 200, 250, 300, 350, 399) THEN

                IF NEW.daemon_id != 0 THEN
                    UPDATE mxq_daemon SET
                        mtime = NULL,
                        daemon_slots_running = daemon_slots_running - NEW.host_slots,
                        daemon_jobs_running  = daemon_jobs_running  - 1
                    WHERE daemon_id             = NEW.daemon_id
                      AND daemon_slots_running >= NEW.host_slots
                      AND daemon_jobs_running  >= 1;
                END IF;

                UPDATE mxq_group SET
                    group_slots_running   = group_slots_running - OLD.host_slots,
                    group_jobs_running    = group_jobs_running  - 1,
                    group_jobs_failed     = group_jobs_failed   + 1,
                    stats_max_sumrss      = GREATEST(stats_max_sumrss,    NEW.stats_max_sumrss),
                    stats_max_maxrss      = GREATEST(stats_max_maxrss,    NEW.stats_maxrss),
                    stats_max_utime_sec   = GREATEST(stats_max_utime_sec, NEW.stats_utime_sec),
                    stats_max_stime_sec   = GREATEST(stats_max_stime_sec, NEW.stats_stime_sec),
                    stats_max_real_sec    = GREATEST(stats_max_real_sec,  NEW.stats_real_sec),
                    stats_total_utime_sec = stats_total_utime_sec + NEW.stats_utime_sec,
                    stats_total_stime_sec = stats_total_stime_sec + NEW.stats_stime_sec,
                    stats_total_real_sec  = stats_total_real_sec  + NEW.stats_real_sec
                WHERE group_id = NEW.group_id;

            -- INQ(0) | ASSIGNED(100) | CANCELLING(989) -> CANCELLED(990)
            ELSEIF NEW.job_status = 990 AND OLD.job_status IN (0, 100, 989) THEN

                SET NEW.date_start = NOW();
                SET NEW.date_end   = NEW.date_start;

                UPDATE mxq_group SET
                    group_jobs_inq       = group_jobs_inq       - 1,
                    group_jobs_cancelled = group_jobs_cancelled + 1
                WHERE group_id = NEW.group_id;

            -- LOADED(150) | RUNNING(200) | UNKNOWN_RUN(250) | KILLING(399) -> UNKNOWN(999)
            ELSEIF NEW.job_status = 999 AND OLD.job_status IN (150, 200, 250, 399) THEN

                IF NEW.daemon_id != 0 THEN
                    UPDATE mxq_daemon SET
                        mtime = NULL,
                        daemon_slots_running = daemon_slots_running - NEW.host_slots,
                        daemon_jobs_running  = daemon_jobs_running  - 1
                    WHERE daemon_id             = NEW.daemon_id
                      AND daemon_slots_running >= NEW.host_slots
                      AND daemon_jobs_running  >= 1;
                END IF;

                UPDATE mxq_group SET
                    group_slots_running = group_slots_running - OLD.host_slots,
                    group_jobs_running  = group_jobs_running  - 1,
                    group_jobs_unknown  = group_jobs_unknown  + 1
                WHERE group_id = NEW.group_id;

            -- UNKNOWN(999) -> KILLED(400) | FAILED(750) | UNKNWON_PRE(755)
            ELSEIF NEW.job_status = 999 AND OLD.job_status IN (400, 750, 755) THEN

                UPDATE mxq_group SET
                   group_jobs_failed  = group_jobs_failed  - 1,
                   group_jobs_unknown = group_jobs_unknown + 1
                WHERE group_id = NEW.group_id;

            -- LOADED(150) | RUNNING(200) | UNKNOWN_RUN(250) | EXTRUNNING(300) | STOPPED(350) | KILLING(399) -> FINISHED(1000)
            ELSEIF NEW.job_status = 1000 AND OLD.job_status IN (150, 200, 250, 300, 350, 399) THEN

                IF NEW.daemon_id != 0 THEN
                    UPDATE mxq_daemon SET
                        mtime = NULL,
                        daemon_slots_running = daemon_slots_running - NEW.host_slots,
                        daemon_jobs_running  = daemon_jobs_running  - 1
                    WHERE daemon_id             = NEW.daemon_id
                      AND daemon_slots_running >= NEW.host_slots
                      AND daemon_jobs_running  >= 1;
                END IF;

                UPDATE mxq_group SET
                    group_slots_running = group_slots_running - OLD.host_slots,
                    group_jobs_running  = group_jobs_running  - 1,
                    group_jobs_finished = group_jobs_finished + 1,
                    stats_max_sumrss    = GREATEST(stats_max_sumrss,    NEW.stats_max_sumrss),
                    stats_max_maxrss    = GREATEST(stats_max_maxrss,    NEW.stats_maxrss),
                    stats_max_utime_sec = GREATEST(stats_max_utime_sec, NEW.stats_utime_sec),
                    stats_max_stime_sec = GREATEST(stats_max_stime_sec, NEW.stats_stime_sec),
                    stats_max_real_sec  = GREATEST(stats_max_real_sec,  NEW.stats_real_sec),
                    stats_total_utime_sec = stats_total_utime_sec + NEW.stats_utime_sec,
                    stats_total_stime_sec = stats_total_stime_sec + NEW.stats_stime_sec,
                    stats_total_real_sec  = stats_total_real_sec  + NEW.stats_real_sec,
                    stats_total_utime_sec_finished = stats_total_utime_sec_finished + NEW.stats_utime_sec,
                    stats_total_stime_sec_finished = stats_total_stime_sec_finished + NEW.stats_stime_sec,
                    stats_total_real_sec_finished  = stats_total_real_sec_finished  + NEW.stats_real_sec
                WHERE group_id = NEW.group_id;

            -- * -> NOT IN [ KILLING(399) | UNKNOWN_PRE(755) | CANCELLING(989) | CANCELLED(990) ]
            ELSEIF NEW.job_status NOT IN (399, 755, 989, 990) THEN

                UPDATE mxq_group SET
                   stats_max_sumrss    = GREATEST(stats_max_sumrss, NEW.stats_max_sumrss),
                   stats_max_maxrss    = GREATEST(stats_max_maxrss, NEW.stats_maxrss),
                   stats_max_utime_sec = GREATEST(stats_max_utime_sec, NEW.stats_utime_sec),
                   stats_max_stime_sec = GREATEST(stats_max_stime_sec, NEW.stats_stime_sec),
                   stats_max_real_sec  = GREATEST(stats_max_real_sec, NEW.stats_real_sec),
                   stats_total_utime_sec = stats_total_utime_sec+NEW.stats_utime_sec,
                   stats_total_stime_sec = stats_total_stime_sec+NEW.stats_stime_sec,
                   stats_total_real_sec  = stats_total_real_sec+NEW.stats_real_sec
                WHERE group_id = NEW.group_id;

            END IF;
        END IF;
    END;
|
DELIMITER ;
UNLOCK TABLES;

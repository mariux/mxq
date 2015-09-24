
DROP TABLE mxq_group;
CREATE TABLE IF NOT EXISTS mxq_group (
   group_id       INT8 UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
   group_name     VARCHAR(511)  NOT NULL DEFAULT 'default',
   group_status   INT1 UNSIGNED NOT NULL DEFAULT 0,
   group_flags    INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_priority INT2 UNSIGNED NOT NULL DEFAULT 127,

   user_uid        INT4 UNSIGNED   NOT NULL,
   user_name       VARCHAR(255)    NOT NULL,
   user_gid        INT4 UNSIGNED   NOT NULL,
   user_group      VARCHAR(255)    NOT NULL,

   job_command    VARCHAR(4095)   NOT NULL,

   job_threads    INT2 UNSIGNED NOT NULL DEFAULT 1,
   job_memory     INT8 UNSIGNED NOT NULL DEFAULT 1024,
   job_time       INT4 UNSIGNED NOT NULL DEFAULT 15,

   job_max_per_node     INT2 UNSIGNED NOT NULL DEFAULT 0,

   group_jobs           INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_jobs_inq       INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_jobs_running   INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_jobs_finished  INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_jobs_failed    INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_jobs_cancelled INT8 UNSIGNED NOT NULL DEFAULT 0,
   group_jobs_unknown   INT8 UNSIGNED NOT NULL DEFAULT 0,

   group_jobs_restarted INT8 UNSIGNED NOT NULL DEFAULT 0,

   group_slots_running INT8 UNSIGNED NOT NULL DEFAULT 0,

   group_mtime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

   group_date_end    TIMESTAMP NOT NULL DEFAULT 0,

   stats_max_maxrss     INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_max_utime_sec  INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_max_stime_sec  INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_max_real_sec   INT8 UNSIGNED NOT NULL DEFAULT 0,

   stats_total_utime_sec    INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_total_stime_sec    INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_total_real_sec     INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_total_wait_sec     INT8 UNSIGNED NOT NULL DEFAULT 0,

   stats_wait_sec     INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_run_sec      INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_idle_sec     INT8 UNSIGNED NOT NULL DEFAULT 0,

   stats_total_utime_sec_finished   INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_total_stime_sec_finished   INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_total_real_sec_finished    INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_total_wait_sec_finished    INT8 UNSIGNED NOT NULL DEFAULT 0,

   depends_on_group     INT8 UNSIGNED NULL DEFAULT NULL,
   dependency_of_group  INT8 UNSIGNED NULL DEFAULT NULL,

   INDEX(group_id),
   INDEX(group_name)
);

DROP TABLE mxq_job;
CREATE TABLE IF NOT EXISTS mxq_job (
   job_id         INT8 UNSIGNED   NOT NULL PRIMARY KEY AUTO_INCREMENT,
   job_status     INT2 UNSIGNED   NOT NULL DEFAULT 0,
   job_flags      INT8 UNSIGNED   NOT NULL DEFAULT 0,
   job_priority   INT2 UNSIGNED   NOT NULL DEFAULT 127,

   group_id       INT8 UNSIGNED   NOT NULL,

   job_workdir    VARCHAR(4095)   NOT NULL,
   job_argc       INT2 UNSIGNED   NOT NULL,
   job_argv       VARCHAR(40959)  NOT NULL,

   job_stdout     VARCHAR(4095)   NOT NULL DEFAULT '/dev/null',
   job_stderr     VARCHAR(4095)   NOT NULL DEFAULT '/dev/null',

   job_umask      INT4            NOT NULL,

   host_submit    VARCHAR(64)     NOT NULL DEFAULT "localhost",

   server_id      VARCHAR(1023)   NOT NULL DEFAULT "",
   host_id        VARCHAR(1023)   NOT NULL DEFAULT "",

   host_hostname  VARCHAR(64)     NOT NULL DEFAULT "",
   host_pid       INT4 UNSIGNED   NOT NULL DEFAULT 0,
   host_slots     INT4 UNSIGNED   NOT NULL DEFAULT 0,

   date_submit  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   date_start   TIMESTAMP NOT NULL DEFAULT 0,
   date_end     TIMESTAMP NOT NULL DEFAULT 0,

   job_id_new     INT8 UNSIGNED   NULL DEFAULT NULL,
   job_id_old     INT8 UNSIGNED   NULL DEFAULT NULL,
   job_id_first   INT8 UNSIGNED   NULL DEFAULT NULL,

   stats_status   INT4 UNSIGNED NOT NULL DEFAULT 0,

   stats_utime_sec  INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_utime_usec INT4 UNSIGNED NOT NULL DEFAULT 0,
   stats_stime_sec  INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_stime_usec INT4 UNSIGNED NOT NULL DEFAULT 0,
   stats_real_sec   INT8 UNSIGNED NOT NULL DEFAULT 0,
   stats_real_usec  INT4 UNSIGNED NOT NULL DEFAULT 0,

   stats_maxrss   INT8 NOT NULL DEFAULT 0,
   stats_minflt   INT8 NOT NULL DEFAULT 0,
   stats_majflt   INT8 NOT NULL DEFAULT 0,
   stats_nswap    INT8 NOT NULL DEFAULT 0,
   stats_inblock  INT8 NOT NULL DEFAULT 0,
   stats_oublock  INT8 NOT NULL DEFAULT 0,
   stats_nvcsw    INT8 NOT NULL DEFAULT 0,
   stats_nivcsw   INT8 NOT NULL DEFAULT 0,

   INDEX (job_id),
   INDEX (group_id),
   INDEX (job_status),
   INDEX (job_priority),
   INDEX (host_hostname(64)),
   INDEX (server_id(767))
);

DROP TABLE mxq_server;
CREATE TABLE IF NOT EXISTS mxq_server (
   host_id          INT4 UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
   host_hostname    VARCHAR(511)  NOT NULL DEFAULT 'localhost',

   server_id        VARCHAR(511)  NOT NULL DEFAULT 'default',

   host_slots       INT2 UNSIGNED NOT NULL DEFAULT 1,
   host_memory      INT8 UNSIGNED NOT NULL DEFAULT 1024,
   host_time        INT4 UNSIGNED NOT NULL DEFAULT 15,

   host_jobs_running    INT2 UNSIGNED NOT NULL DEFAULT 0,
   host_slots_running   INT2 UNSIGNED NOT NULL DEFAULT 0,

   host_mtime       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

   server_start     TIMESTAMP DEFAULT 0,
   server_stop      TIMESTAMP DEFAULT 0,
);



LOCK TABLES mxq_job WRITE, mxq_group WRITE;
DELIMITER |
DROP TRIGGER mxq_add_group|
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
DROP TRIGGER mxq_update_group|
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
DROP TRIGGER mxq_add_job|
CREATE TRIGGER mxq_add_job AFTER INSERT ON mxq_job
    FOR EACH ROW BEGIN
        UPDATE mxq_group SET
            group_jobs=group_jobs+1,
            group_jobs_inq=group_jobs_inq+1,
            group_mtime=NULL
        WHERE group_id=NEW.group_id;
    END;
|
DROP TRIGGER mxq_update_job|
CREATE TRIGGER mxq_update_job BEFORE UPDATE ON mxq_job
    FOR EACH ROW BEGIN
        IF NEW.job_status != OLD.job_status THEN
            IF NEW.job_status IN (150, 200) AND OLD.job_status IN (0, 100)  THEN
                UPDATE mxq_group SET
                   group_jobs_inq=group_jobs_inq-1,
                   group_jobs_running=group_jobs_running+1,
                   group_slots_running=group_slots_running+NEW.host_slots,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status = 200 AND OLD.job_status = 150 THEN
                UPDATE mxq_group SET
                   group_slots_running=group_slots_running-OLD.host_slots+NEW.host_slots,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status IN (400, 750) AND OLD.job_status IN (150, 200, 250, 300, 350, 399) THEN
                UPDATE mxq_group SET
                   group_slots_running=group_slots_running-NEW.host_slots,
                   group_jobs_running=group_jobs_running-1,
                   group_jobs_failed=group_jobs_failed+1,
                   stats_max_maxrss=GREATEST(stats_max_maxrss, NEW.stats_maxrss),
                   stats_max_utime_sec=GREATEST(stats_max_utime_sec, NEW.stats_utime_sec),
                   stats_max_stime_sec=GREATEST(stats_max_stime_sec, NEW.stats_stime_sec),
                   stats_max_real_sec=GREATEST(stats_max_real_sec, NEW.stats_real_sec),
                   stats_total_utime_sec=stats_total_utime_sec+NEW.stats_utime_sec,
                   stats_total_stime_sec=stats_total_stime_sec+NEW.stats_stime_sec,
                   stats_total_real_sec=stats_total_real_sec+NEW.stats_real_sec,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status = 990 AND OLD.job_status IN (0, 100, 989) THEN
                SET NEW.date_start = NOW();
                SET NEW.date_end = NEW.date_start;
                UPDATE mxq_group SET
                   group_jobs_inq=group_jobs_inq-1,
                   group_jobs_cancelled=group_jobs_cancelled+1,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status = 999 AND OLD.job_status IN (150, 200, 250, 399) THEN
                UPDATE mxq_group SET
                   group_slots_running=group_slots_running-NEW.host_slots,
                   group_jobs_running=group_jobs_running-1,
                   group_jobs_unknown=group_jobs_unknown+1,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status = 999 AND OLD.job_status IN (400, 750, 755) THEN
                UPDATE mxq_group SET
                   group_jobs_failed=group_jobs_failed-1,
                   group_jobs_unknown=group_jobs_unknown+1,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status = 1000 AND OLD.job_status IN (150, 200, 250, 300, 350, 399) THEN
                UPDATE mxq_group SET
                   group_slots_running=group_slots_running-NEW.host_slots,
                   group_jobs_running=group_jobs_running-1,
                   group_jobs_finished=group_jobs_finished+1,
                   stats_max_maxrss=GREATEST(stats_max_maxrss, NEW.stats_maxrss),
                   stats_max_utime_sec=GREATEST(stats_max_utime_sec, NEW.stats_utime_sec),
                   stats_max_stime_sec=GREATEST(stats_max_stime_sec, NEW.stats_stime_sec),
                   stats_max_real_sec=GREATEST(stats_max_real_sec, NEW.stats_real_sec),
                   stats_total_utime_sec=stats_total_utime_sec+NEW.stats_utime_sec,
                   stats_total_stime_sec=stats_total_stime_sec+NEW.stats_stime_sec,
                   stats_total_real_sec=stats_total_real_sec+NEW.stats_real_sec,
                   stats_total_utime_sec_finished=stats_total_utime_sec_finished+NEW.stats_utime_sec,
                   stats_total_stime_sec_finished=stats_total_stime_sec_finished+NEW.stats_stime_sec,
                   stats_total_real_sec_finished=stats_total_real_sec_finished+NEW.stats_real_sec,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            ELSEIF NEW.job_status NOT IN (399, 755, 989, 990) THEN
                UPDATE mxq_group SET
                   stats_max_maxrss=GREATEST(stats_max_maxrss, NEW.stats_maxrss),
                   stats_max_utime_sec=GREATEST(stats_max_utime_sec, NEW.stats_utime_sec),
                   stats_max_stime_sec=GREATEST(stats_max_stime_sec, NEW.stats_stime_sec),
                   stats_max_real_sec=GREATEST(stats_max_real_sec, NEW.stats_real_sec),
                   stats_total_utime_sec=stats_total_utime_sec+NEW.stats_utime_sec,
                   stats_total_stime_sec=stats_total_stime_sec+NEW.stats_stime_sec,
                   stats_total_real_sec=stats_total_real_sec+NEW.stats_real_sec,
                   group_mtime=NULL
                WHERE group_id=NEW.group_id;
            END IF;
        END IF;
    END;
|
DELIMITER ;
UNLOCK TABLES;

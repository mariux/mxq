
-- fix stats_wait_sec, stats_run_sec, stats_idle_sec in mxq_group
-- for all finished groups (group_date_end > 0)

-- do not touch active jobs

DROP TEMPORARY TABLE IF EXISTS mytemp;

SET @sinq=0,@srun=0,@gid=0,@dt=0,@ot=0;

CREATE TEMPORARY TABLE IF NOT EXISTS mytemp AS
(
    SELECT
        group_id,
        MAX(IF(last = "run", dtime, 0))  AS run,
        MAX(IF(last = "wait", dtime, 0)) AS wait,
        MAX(IF(last = "idle", dtime, 0)) AS idle,
        MAX(date_end) AS date_end,
        group_date_end,
        stats_run_sec,
        stats_wait_sec,
        stats_idle_sec
    FROM
        (
            SELECT
                gidchange,
                group_id,
                time,
                sinq,
                srun,
                phase,
                last,
                SUM(dt) AS dtime,
                MAX(date_end) AS date_end,
                group_date_end,
                stats_wait_sec,
                stats_run_sec,
                stats_idle_sec
            FROM
                (
                    SELECT
                        @gidchange := @gid != group_id AS gidchange,
                        @gid := group_id AS group_id,
                        time,
                        sdinq,
                        sdrun,
                        @sinq := IF(@gidchange = 1,
                                    sdinq,
                                    @sinq + sdinq
                                ) AS sinq,
                        @srun := IF(@gidchange = 1,
                                    sdrun,
                                    @srun + sdrun
                                ) AS srun,
                        IF(@gidchange, "new", @last) as last,
                        @last := IF(@srun > 0,
                                        "run",
                                        IF(@sinq > 0,
                                                "wait",
                                                "idle"
                                        )
                                ) AS phase,
                        @dt   := IF(@gidchange,
                                    0,
                                    time - @ot
                                ) AS dt,
                        @ot   := time,
                        date_end,
                        group_date_end,
                        stats_wait_sec,
                        stats_run_sec,
                        stats_idle_sec
                    FROM
                        (
                            SELECT
                                group_id,
                                time,
                                SUM(dinq) as sdinq,
                                SUM(drun) as sdrun,
                                MAX(date_end) AS date_end,
                                group_date_end,
                                stats_wait_sec,
                                stats_run_sec,
                                stats_idle_sec
                            FROM
                                (
                                    (
                                        SELECT
                                            mxq_job.group_id AS group_id,
                                            job_status,
                                            unix_timestamp(date_submit) AS time,
                                            1 AS event,
                                            1 AS dinq,
                                            0 AS drun,
                                            date_end,
                                            group_date_end,
                                            stats_wait_sec,
                                            stats_run_sec,
                                            stats_idle_sec
                                        FROM
                                            mxq_job, mxq_group
                                        WHERE
                                            mxq_job.group_id = mxq_group.group_id
                                    )
                                    UNION ALL
                                    (
                                        SELECT
                                            mxq_job.group_id AS group_id,
                                            job_status,
                                            unix_timestamp(date_start),
                                            2,
                                            -1,
                                            1,
                                            date_end,
                                            group_date_end,
                                            stats_wait_sec,
                                            stats_run_sec,
                                            stats_idle_sec
                                        FROM
                                            mxq_job, mxq_group
                                        WHERE
                                            mxq_job.group_id = mxq_group.group_id
                                            AND date_start > 0
                                    )
                                    UNION ALL
                                    (
                                        SELECT
                                            mxq_job.group_id AS group_id,
                                            job_status,
                                            unix_timestamp(date_end),
                                            3,
                                            0,
                                            -1,
                                            date_end,
                                            group_date_end,
                                            stats_wait_sec,
                                            stats_run_sec,
                                            stats_idle_sec
                                        FROM
                                            mxq_job, mxq_group
                                        WHERE
                                            mxq_job.group_id = mxq_group.group_id
                                            AND date_end > 0
                                    )
                                    ORDER BY
                                        group_id,
                                        time,
                                        event
                                ) AS S1
                            GROUP BY
                                group_id, time
                            ORDER BY
                                group_id, time
                        ) AS S2
                ) AS S3
            WHERE
                    last != "new"
            GROUP BY
                group_id, last
            ORDER BY
                group_id, last
        ) AS S4
    GROUP BY
        group_id
)
;

SET @sinq=0,@srun=0,@gid=0,@dt=0,@ot=0;

UPDATE mxq_group AS g
    LEFT JOIN mytemp AS t
    ON g.group_id = t.group_id
    SET
        g.stats_wait_sec = wait,
        g.stats_run_sec = run,
        g.stats_idle_sec = idle
    WHERE t.group_id
        AND t.group_date_end
        AND g.group_date_end
;

DROP TEMPORARY TABLE IF EXISTS mytemp;

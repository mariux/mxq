
LOCK TABLES mxq_group WRITE, mxq_job WRITE;

UPDATE
    mxq_group
SET
    group_slots_running = 0,
    group_sum_starttime = 0;

UPDATE
    mxq_group
JOIN (
    SELECT
        group_id,
        SUM(S) AS rs,
        SUM(SS) AS ss
    FROM (
        SELECT
            group_id,
            host_slots,
            COUNT(job_id) AS N,
            SUM(host_slots) AS S,
            SUM(UNIX_TIMESTAMP(date_start)) AS SS
        FROM
            mxq_job
        WHERE
            job_status IN (150, 200)
        GROUP BY
            group_id,
            host_slots
        ) AS ANONYM
    GROUP BY
        group_id
    ) AS J
    ON
        mxq_group.group_id = J.group_id
    SET
        group_slots_running = rs,
        group_sum_starttime = ss;

UNLOCK TABLES;

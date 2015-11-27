
LOCK TABLES mxq_group AS G WRITE, mxq_job AS J WRITE;
UPDATE mxq_group AS G
  INNER JOIN (
    SELECT
        group_id,
        SUM(UNIX_TIMESTAMP(date_start)) * host_slots AS group_sum_starttime
    FROM
        mxq_job AS J
    WHERE
        job_status = 200
    GROUP BY
        group_id
  ) AS J ON J.group_id = G.group_id
SET G.group_sum_starttime = J.group_sum_starttime;
UNLOCK TABLES;

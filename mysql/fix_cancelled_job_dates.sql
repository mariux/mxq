UPDATE mxq_job
    JOIN mxq_group
        ON mxq_job.group_id = mxq_group.group_id
    SET mxq_job.date_start = mxq_group.group_date_end,
        mxq_job.date_end = mxq_group.group_date_end
    WHERE job_status = 990
        AND (date_start = 0 OR date_end = 0);


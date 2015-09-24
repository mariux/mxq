ALTER TABLE mxq_group
    ADD COLUMN
        job_max_per_node  INT2 UNSIGNED NOT NULL DEFAULT 0
    AFTER
        job_time;

ALTER TABLE mxq_job
    ADD COLUMN
        host_id VARCHAR(1023) NOT NULL DEFAULT ""
    AFTER
        server_id;

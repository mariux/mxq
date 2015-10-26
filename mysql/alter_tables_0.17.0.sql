ALTER TABLE mxq_group
    ADD COLUMN
        stats_max_sumrss  INT8 UNSIGNED NOT NULL DEFAULT 0
    AFTER
        group_date_end;

ALTER TABLE mxq_job
    ADD COLUMN
        stats_max_sumrss  INT8 UNSIGNED NOT NULL DEFAULT 0
    AFTER
        job_id_first;

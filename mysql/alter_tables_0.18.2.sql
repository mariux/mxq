ALTER TABLE mxq_job
    ADD COLUMN
       host_cpu_set   VARCHAR(4095)   NOT NULL DEFAULT ""
    AFTER
        host_slots;

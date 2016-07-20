ALTER TABLE mxq_daemon
    ADD COLUMN
        daemon_maxtime INT8 UNSIGNED NOT NULL  DEFAULT 0
    AFTER daemon_memory;

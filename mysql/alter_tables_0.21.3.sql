
ALTER TABLE mxq_group
    MODIFY COLUMN
        group_name  VARCHAR(128) NOT NULL DEFAULT 'default',
    MODIFY COLUMN
        user_name   VARCHAR(256) NOT NULL,
    MODIFY COLUMN
        user_group  VARCHAR(256) NOT NULL,
    MODIFY COLUMN
        job_command VARCHAR(256) NOT NULL;

ALTER TABLE mxq_job
    MODIFY COLUMN
        job_workdir    VARCHAR(4096)   NOT NULL,
    MODIFY COLUMN
        job_argc       INT4 UNSIGNED   NOT NULL,
    MODIFY COLUMN
        job_argv       VARCHAR(32768)  NOT NULL,
    MODIFY COLUMN
        job_stdout     VARCHAR(4096)   NOT NULL DEFAULT '/dev/null',
    MODIFY COLUMN
        job_stderr     VARCHAR(4096)   NOT NULL DEFAULT '/dev/null',
    MODIFY COLUMN
        server_id      VARCHAR(64)     NOT NULL DEFAULT "",
    MODIFY COLUMN
        host_id        VARCHAR(128)    NOT NULL DEFAULT "",
    MODIFY COLUMN
        host_cpu_set   VARCHAR(4096)   NOT NULL DEFAULT "",
    DROP INDEX
        server_id,
    ADD INDEX
        server_id (server_id(64));

ALTER TABLE mxq_job
    ADD INDEX
        host_id (host_id(128));

DROP TABLE IF EXISTS mxq_server;

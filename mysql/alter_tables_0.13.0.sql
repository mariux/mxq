ALTER TABLE mxq_group
    ADD COLUMN (
        depends_on_group    INT8 UNSIGNED NULL DEFAULT NULL,
        dependency_of_group INT8 UNSIGNED NULL DEFAULT NULL
    );

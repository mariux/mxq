ALTER TABLE mxq_group
    ADD COLUMN
        group_sum_starttime INT8 UNSIGNED NOT NULL DEFAULT 0
      AFTER group_slots_running;


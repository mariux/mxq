UPDATE mxq_job
    SET
        host_id = CONCAT(host_hostname, '::', server_id)
    WHERE
        host_id = ""
    AND server_id != ""
    AND host_hostname != "";

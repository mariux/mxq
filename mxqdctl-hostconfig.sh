#!/bin/bash

hostconfig=/etc/hostconfig
shorthost=${HOSTNAME%.molgen.mpg.de}
declare -i started=0

mxqd=mxqd

defaultargs=(--daemonize)
pidfilebase=/dev/shm/mxqdctl-hostconfig

shopt -s nullglob

function start_all_hostconfig()
{
    while read -a line ; do
        host=${line[0]}
        var=${line[1]}

        unset 'line[0]'
        unset 'line[1]'


        if [ "${var}" != "mxqd" ] ; then
            continue
        fi

        if [ "${host}" != "${shorthost}" ] ; then
            continue
        fi

        args=(${defaultargs[@]} --pid-file "${pidfilebase}${started}.pid" ${line[@]})

        echo "executing ${mxqd} ${args[@]}"

        ${mxqd} ${args[@]}

        started+=1
    done < ${hostconfig}

    if [ ${started} -lt 1 ] ; then
        echo >&2 "host '${shorthost}' is not configured for mxqd in '${hostconfig}'."
        exit 1
    fi
}

function stop_all_started()
{
    for pidfile in ${pidfilebase}* ; do
        ouid=$(stat --format "%u" "${pidfile}")
        if [ "${UID}" != "${ouid}" ] ; then
            continue
        fi
        pid=$(cat ${pidfile})
        echo "${pidfile}: killing ${pid}"
        kill ${pid}
    done
}

case "${BASH_ARGV[0]}" in
    start)
        start_all_hostconfig
        ;;
    stop)
        stop_all_started
        ;;
    stopall)
        killall -u "${USER}" "${mxqd}"
        ;;
esac


#!/bin/bash

status=0
msg=""
for server in $(barman list-server | awk '{print $1}')
do
    last_backup=$(barman list-backup "$server" | head -n1 | awk '{print $2}')

    first_xlog=$(barman show-backup "$server" "${last_backup}" | awk '/Begin WAL/ {print $NF}')
    last_xlog=$(barman show-backup "$server" "${last_backup}" | awk '/Last available/ {print $NF}')

    if [ -z "${first_xlog}" -o -z "${last_xlog}" ]; then
        status=1
        msg="${msg}Could not get xlogs info for $server. "
        continue
    fi

    if [ "${first_xlog}" == "${last_xlog}" ]; then
       continue
    fi
    #echo "$server ${first_xlog} ${last_xlog}"

    dir=$(barman show-server "$server" | awk '/backup_directory/ {print $NF}')

    first_tli=${first_xlog:0:8}
    last_tli=${last_xlog:0:8}
    if [ "${first_tli}" != "${last_tli}" ]; then
        status=1
        msg="${msg}Timeline switched for $server cluster, not checking it xlogs. "
        continue
    fi

    first_log=$(echo "ibase=16; obase=A; ${first_xlog:8:8}" | bc)
    first_seg=$(echo "ibase=16; obase=A; ${first_xlog:16}" | bc)
    last_log=$(echo "ibase=16; obase=A; ${last_xlog:8:8}" | bc)
    last_seg=$(echo "ibase=16; obase=A; ${last_xlog:16}" | bc)

    count=0
    for log in $(seq "${first_log}" "${last_log}")
    do
        if [ "${log}" -eq "${first_log}" ]; then
            start=${first_seg}
        else
            start=0
        fi

        if [ "${log}" -eq "${last_log}" ]; then
            end=${last_seg}
        else
            end=255
        fi
        prefix=$(printf "%s%08X" "${first_tli}" "${log}")
        #echo "Processing ${prefix} xlogs for $server."

        for seg in $(seq $start $end)
        do
            xlog_name=$(printf "%s%08X\n" "${prefix}" "$seg")
            size=$(stat --format %s "/${dir}/wals/${prefix}/${xlog_name}" 2>/dev/null)
            if [ $? -ne 0 -o "x$size" != "x16777216" ]; then
                #echo "Problem with ${xlog_name} of ${server} cluster."
                count=$((count+1))
            fi
        done
    done

    if [ $count -ne 0 ]; then
        msg="${msg}Cluster $server does not have $count needed valid xlogs. "
        status=1
    fi
done

ts=$(date +%s)
if [ -z "$msg" ]; then msg="Everything is OK."; fi
echo "${ts};${status};${msg}" > /tmp/check_xlogs.status

#!/bin/bash

#set -x

#err_pipe="/tmp/err_pipe_${RANDOM}"
#mkfifo ${err_pipe}
#
#if [[ ! -p ${err_pipe} ]]; then
#    echo Failed to create pipe ${err_pipe} 1>&2
#    exit 1
#fi
#
#trap "rm -f ${err_pipe}" SIGINT SIGTERM

function usage {
    echo "Usage: `basename $0` [-dh]"
    echo
    echo "Run all RepCRec tests."
    echo
    echo "OPTIONS"
    echo "-------"
    echo "   -d   show debug output and log errors"
    echo "   -h   show help"
    echo
}

debug_only=1

# Parse arguments.
args=`getopt adh $*`
if [ $? != 0 ]
then
    echo error : options failed to parse
    usage
    exit 2
fi
set -- $args
for i; do
    case "$i" in
        -d) debug_only=0;
            shift;;
        -h) usage; exit 0;;
        --) shift; break;;
    esac
done
if [[ -n "$*" ]]; then
    echo error : detected extraneous positional parameters [ "$*" ] 1>&2
    usage
    exit 2
fi

base_dir=`dirname $0`
test_dir="${base_dir}/test_data"
bin_dir="${base_dir}/bin"
test_cmd="python ${bin_dir}/repcrec"

# Run all test files.
ls $test_dir/test[0-9]* | while read -r file; do
    echo "************************************************************"
    echo "******************** Running ${file}"

    data_dir=/tmp/test_${RANDOM}
    error_header_printed=1
    if [[ 0 == ${debug_only} ]]; then
        PYTHONPATH=${base_dir} ${test_cmd} -f ${file} ${data_dir} 2>&1 | grep '^debug'
        if [[ $? != 0 ]]; then
            echo 'ERROR during test; run with full output to see error trace'
        fi
    else
        PYTHONPATH=${base_dir} ${test_cmd} -f ${file} ${data_dir} 2>&1
    fi

    echo "******************** Completed ${file}"
    echo "************************************************************"
    echo ''
done


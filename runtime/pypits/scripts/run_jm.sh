#!/bin/bash

SPITSDIR="$PWD"
JOBMANAGER="${JOBMANAGER:-jm.py}"
JOBMANAGER_ARGS="${JOBMANAGER_ARGS:-""}"
SPITSBIN="${SPITSBIN:-""}"
SPITSARGS="${SPITSARGS:-""}"
PYTHONCMD="${PYTHONCMD:-python}"
JOBID="${JOBID:-job-123dasilva4}"
JOBDIR="${JOBDIR:-${HOME}/spits-jobs/$JOBID}"
NODESDIR="$JOBDIR/nodes"
LOGSDIR="$JOBDIR/logs"

## Generate unique spits ID used for logs
ADDR=$(hostname)
SPITSUID="$ADDR"-"$$"
OUTPUTFILE="$LOGSDIR/jobmanager-$SPITSUID.stdout"
LOGFILE="$LOGSDIR/jobmanager-$SPITSUID.stderr"

exit_error() {
    echo -e "\e[91m$1\nExiting...\e[0m"
    exit 1
}

check_job_dir() {
    echo -e "\e[1mChecking Job directory '$JOBDIR'...\e[21m"
    if [ ! -d "$JOBDIR" ]; then
        echo "Job directory '$JOBDIR' does not exists\nCreating a new one..."
        mkdir -p $JOBDIR || exit_error "Error creating directory $JOBDIR"
    else
        echo "The job directory '$JOBDIR' already exists!"
    fi

    if [ -f "$JOBDIR/jm.exists" ]; then
        exit_error "There is already a job manager instantiated for the job $JOBID\nIf the jm.finished file is 1. The job has already terminated and results shold be in log folder. Else the job manager may failed to execute in some point. Remove this jm.exists file on the Job Directory to restart the job or delete the folder to start again."
    fi

    if [ ! -d "$NODESDIR" ]; then
        echo "Creating nodes directory at '$NODESDIR'"
        mkdir -p $NODESDIR || exit_error "Error creating directory $NODESDIR"
    fi

    if [ ! -d "$LOGDIR" ]; then
        echo "Creating logs directory at '$LOGSDIR'"
        mkdir -p $LOGSDIR || exit_error "Error creating directory $LOGSDIR"
    fi

    return 0
}

show_script_params() {
    echo -e "Starting script with parameters:"
    echo -e "\t\e[1mUsing '$SPITSUID' as UID\e[21m"
    echo -e "\t\e[1mUsing '$SPITSDIR' as Default Spits Directory\e[21m"
    echo -e "\t\e[1mJobManager file\e[21m: $JOBMANAGER"
    echo -e "\t\e[1mJobManager arguments\e[21m: $JOBMANAGER_ARGS"
    echo -e "\t\e[1mJob ID\e[21m: $JOBID"
    echo -e "\t\e[1mJob directory\e[21m: $JOBDIR"
    echo -e "\t\e[1mNodes directory\e[21m: $NODESDIR"
    echo -e "\t\e[1mLogs directory\e[21m: $LOGSDIR"
    echo -e "\t\e[1mOutput file\e[21m: $OUTPUTFILE"
    echo -e "\t\e[1mError file\e[21m: $LOGFILE"
    echo -e "\t\e[1mPython command\e[21m: $PYTHONCMD"
    return 0
}

run_jobmanager() {
    JOBMANAGER_ARGS+="--killtms=1 --log=$LOGFILE --verbose=1 --jobid=$JOBID --net-port=7726"
    SPITSBIN+="/media/napoli/d9b9ded0-5c92-4552-9238-9aaba7ffb8e6/Documents/Douturado/spits/bin/spits-pi/spits-pi-module"
    SPITSARGS+="100000"

    JM_CMD="$PYTHONCMD $SPITSDIR/$JOBMANAGER $JOBMANAGER_ARGS $SPITSBIN $SPITSARGS"
    echo $SPITSUID > $JOBDIR/jm.exists
    # Wait to NFS sync...
    sleep 1

    if [ "x$SPITSUID" = "x$(cat $JOBDIR/jm.exists)" ]
    then
        echo -e "\e[1mExecuting Job Manager!\e[21m"
        echo -e "Running command: \n$JM_CMD"
        echo 0 > $JOBDIR/jm.finished
        trap 'echo "Caught SIGTERM signal!" && kill -KILL $PID && kill -KILL $PID' SIGTERM SIGINT
        $JM_CMD | tee -ia $OUTPUTFILE &
        PID=$!
        wait $PID
        EXIT_STATUS=$?
    fi

    return $EXIT_STATUS
}

#
# MAIN ROUTINE
#

if [ "$#" -ge 1 ]; then
    exit_error "No parameters needed, must use environment variables!"
fi

show_script_params ||  exit_error
check_job_dir || exit_error "Error checking directory from job!"
run_jobmanager || exit_error "Error in JobManager with code $?"
echo -e "Exited! Returned with code $?"

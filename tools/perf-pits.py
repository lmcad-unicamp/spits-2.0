#!/usr/bin/env python3
import sys, argparse
from datetime import datetime, timedelta
from statistics import mean, stdev


def loadFiles(fList):
  jobs = []
  for i in fList:
    # Create Job
    job = {"Name" : i, "Start Time" :"", "End Init. Time" :"", "Kill Time" :"", "Stats": "", "Threads" : {}}

    #  Parse Log
    try:
      f = open(i)
    except Exception as e:
      print(e)
      exit(1)
    
    job["Kill Time"] = None
    job["Start Time"] = None
    job["Initializing"] = None

    for j in f:
      jVec = j.strip().split(" - ") # [0] = datetime, [1] thread, [2] log type, [3] message
      if(len(jVec) < 4):
        continue
      time = datetime.strptime(jVec[0].strip(), '%Y-%m-%d %H:%M:%S,%f')
      if("Starting workers" in jVec[3]):
        job["Start Time"] = time
      elif("Initializing" in jVec[3]):
        job["Threads"][jVec[1]] = {"Start Time": time, "Stats": "", "Tasks": {}}
      elif("Processing" in jVec[3]):
        taskNum = int(jVec[3][jVec[3].index("task ") + 5 : jVec[3].index(" from job")])
        job["Threads"][jVec[1]]["Tasks"][taskNum] = {"Start Time": time}
      elif("processed" in jVec[3]):
        taskNum = int(jVec[3][jVec[3].index("Task ") + 5 : jVec[3].index(" processed")])
        job["Threads"][jVec[1]]["Tasks"][taskNum]["End Time"] = time
      elif("kill" in jVec[3]):
        job["Kill Time"] = time
    f.close()

    # Handle TMs that were interrupted before receiving the kill signal.
    if job["Kill Time"] == None :
      job["Kill Time"] = time # Receive last time

    if job["Start Time"] == None :
      print("WARNING: Could not find the Start Time on log file \"{}\"".format(i))
      print("It will not be included on the statistics...")
    else:
      jobs.append(job)

  return jobs

def processTasks(jobs):
  tasksGlobalendTime = {}
  for j in jobs:
    firstTaskTime = datetime.now()
    for th in j["Threads"]:
      for task in j["Threads"][th]["Tasks"]:
        currentTask = j["Threads"][th]["Tasks"][task]
        currentTask["Status"] = "undefined"
        firstTaskTime = min(firstTaskTime, currentTask["Start Time"])
        if("End Time" not in currentTask):
          currentTask["Status"] = "unfinished"
          currentTask["Duration"] = j["Kill Time"] - currentTask["Start Time"]
          continue
        tasksGlobalendTime[task] = min(currentTask["End Time"], tasksGlobalendTime.get(task, datetime.now()))
        currentTask["Duration"] = currentTask["End Time"] - currentTask["Start Time"]
    j["End Init. Time"] = firstTaskTime

  for j in jobs:
    j["Stats"] = {
      "Time" :
      {
        "Successful" : timedelta(0),
        "Repeated": timedelta(0),
        "Unfinished": timedelta(0),
        "Init. Overhead": timedelta(0),
        "Total" : timedelta(0)
      }
    }

    for th in j["Threads"]:
      #  Threads statistics
      currentThread = j["Threads"][th]
      currentThread["Stats"] = {
        "Time" :
        {
          "Successful" : timedelta(0),
          "Repeated": timedelta(0),
          "Unfinished": timedelta(0),
          "Total" : j["Kill Time"] - currentThread["Start Time"]
        }
      }

      #  Process tasks
      for task in j["Threads"][th]["Tasks"]:
        currentTask = j["Threads"][th]["Tasks"][task]
        if(currentTask["Status"] != "undefined"):
          currentThread["Stats"]["Time"]["Unfinished"] += currentTask["Duration"]
          continue
        if(currentTask["End Time"] == tasksGlobalendTime[task]):
          currentTask["Status"] = "success"
          currentThread["Stats"]["Time"]["Successful"] += currentTask["Duration"]
        else:
          currentTask["Status"] = "repeated"
          currentThread["Stats"]["Time"]["Repeated"] += currentTask["Duration"]
      j["Stats"]["Time"]["Unfinished"] += currentThread["Stats"]["Time"]["Unfinished"]
      j["Stats"]["Time"]["Successful"] += currentThread["Stats"]["Time"]["Successful"]
      j["Stats"]["Time"]["Repeated"] += currentThread["Stats"]["Time"]["Repeated"]
      j["Stats"]["Time"]["Total"] += currentThread["Stats"]["Time"]["Total"]
    if(len(j["Threads"])):
      j["Stats"]["Time"]["Init. Overhead"] = j["End Init. Time"] - j["Start Time"]

def printStatistics(name, v, total):
  print("%s, %.2f, %.6f, %.6f, %.6f, %.6f, %.6f" % (name, 100*sum(v)/total, sum(v), mean(v), (lambda x:stdev(x) if len(x) > 1 else 0)(v), min(v), max(v)))


def jobStatistics(jobs):
  init_overhead = []
  useful_work = []
  starving = []
  duplicated_work = []

  for j in jobs:
    if(j["Kill Time"] == ""):
      continue
    init_overhead.append(j["Stats"]["Time"]["Init. Overhead"].total_seconds() * len(j["Threads"]))
    useful_work.append(j["Stats"]["Time"]["Successful"].total_seconds())
    duplicated_work.append((j["Stats"]["Time"]["Repeated"] + j["Stats"]["Time"]["Unfinished"]).total_seconds())
    starving.append(j["Stats"]["Time"]["Total"].total_seconds() - duplicated_work[-1] - useful_work[-1] - init_overhead[-1])

  total = sum(init_overhead) + sum(useful_work) + sum(duplicated_work) + sum(starving)
  if(total == 0):
    print("Failed to parse logs")
    return
  print("Metric, %, total, average, std, min, max")
  printStatistics("Init overhead", init_overhead, total)
  printStatistics("Useful work", useful_work, total)
  printStatistics("Duplicated work", duplicated_work, total)
  printStatistics("Starving", starving, total)


description_msg="""    Perf-PITS: A PY-PITS performance diagnosys tool

This tool obtains performance statistics from logs produced by PY-PITS
during the execution of a SPITS job.

Wheneve a task manager (TM) is started, it:
1) creates worker threads (Thread 1, 2, ...);
2) invokes the user defined initialization routine on the Main Thread;
3) once the initialization routine is finished, it signal the Threads to 
   start processing the Tasks that are being sent by the JM; and
4) finishes the threads once the JM signal the job completion.

The following workflow illustrates this process:

                         +-----------------------+
                         | Task Manager Workflow |
                         +-----------------------+

        Start Task      Start processing              Finish processing
          Manager            tasks                          tasks
            |                  |                              |
            v                  v                              v
Main Thread |---Worker init----|                              |

   Thread 1 |--wait for init---|**T1**|..|***T3***|....|**T5**|

   Thread 2 |--wait for init---|..|**T2**|....|***T4***|.|**T5:
                                    ^       ^                 ^
                                   /        |                / 
                            Thread 2        Thread 2        Task T5
                            processing      waiting for     interrupted 
                            task T2         tasks to be     due to job
                                            processed       completion

The following table describes the main metrics used to analyze the
performance of the task managers:

+-----------------+-------------------------------------------------------+
| Metric          | Description                                           |
|-----------------+-------------------------------------------------------|
| Init overhead   | Worker init time x # of threads                       |
|-----------------+-------------------------------------------------------|
| Userful work    | Sum of thread time spent processing tasks.            |
|-----------------+-------------------------------------------------------|
| Starving        | Sum of thread time waiting for tasks to be processed. |
|-----------------+-------------------------------------------------------|
| Duplicated work | Sum of thread time spent processing tasks that were   |
|                 | not committed due to interruption or duplication      |
+-----------------+-------------------------------------------------------+

The system aims at keeping the task manager threads busy processing tasks,
hence, the "Useful work" metric should contain the largest values.

Problems:

** Init overhead is high **
Time spent initializing the task manager is too high when compared to the
time spent performing useful work.

Causes
 P1) Initialization routine is too expensive:
     The tool developer may need to optimize this routine.
     In some cases, it may also be usefull to delegate the initialization
     to the job manager and have it send the initialized data structures
     to all task managers.

 P2) There are too many task managers:
     The more task managers, the less work per task manager. At some point,
     having more task managers reduce the amount of usefull work per TM to
     the level in which the task managers' initialization time becomes relevant.

** Starving time is high **
The job manager (JM) process is not being able to distribute tasks in a rate
that is high enough to keep the task managers busy.

Causes
 P3) The user defined "generate task" function is taking too long to
     generate tasks:
     The tools developer may need to optimize this function.
 P4) The time the job manager takes to send the tasks to the tasks managers
     is too high:
     This may happen because: a) the size of the tasks are too large;
     b) the performance of the network is too low; and/or c) because there
     are too many task managers consumning tasks from a single job manager.

** Duplicated work time is high **
The amount of time executing tasks that are duplicated or not committed
is too high.

Causes
 P5) The number of tasks that can be duplicated by the system is too high:
     The user may adjust this parameter passing an argument to the PY-PITS
     runtime tool.
"""
  

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description=description_msg,
                                   formatter_class=argparse.RawTextHelpFormatter)

  parser.add_argument('files', metavar='SPITS_LOG_FILE', type=str, nargs='+',
                      help='log file to be processed.')

  args = parser.parse_args()

  jobs = loadFiles(args.files)
  gStats = processTasks(jobs)
  jobStatistics(jobs)

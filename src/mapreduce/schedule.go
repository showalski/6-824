package mapreduce

import (
    "fmt"
    "math"
)

type schedPara struct {
    worker  string
    task    int
    err     string
}

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
    paraChan := make(chan schedPara)
    var tasks []int
    var workers []string
    for i := 0; i < ntasks; i ++ {
        tasks = append(tasks, i)
    }

    for {
        // All tasks done.
        if ntasks == 0 {
            break
        }
        select {
        case newWorker := <- registerChan:
            debug("new worker %s available\n", newWorker)
            workers = append(workers, newWorker)
        case para := <- paraChan:
            if para.err == "ok" {
                // Add newly registered worker to available workers
                debug("worker %s finished %d\n", para.worker, para.task)
                workers = append(workers, para.worker)
                ntasks --
            } else {
                // Worker failed. Need reschedule this task. Add it to todo tasks queue
                debug("worker %s failed, %d will be rescheduled\n", para.worker, para.task)
                tasks = append(tasks, para.task)
            }
        }
        var i int
        for i = 0; i < int(math.Min(float64(len(tasks)), float64(len(workers)))); i ++ {
            debug("%d:%s, wkrNum: %d, tskNum: %d\n",tasks[i], workers[i], len(workers), len(tasks))
            go func(jobName, file string, phase jobPhase, taskNum, n_other int, worker string) {
                args := new(DoTaskArgs)
                args.JobName = jobName
                args.File = file
                args.Phase = phase
                args.TaskNumber = taskNum
                args.NumOtherPhase = n_other
                // call worker
                ok := call(worker, "Worker.DoTask", args, new(struct{}))
                if ok == false {
                    debug("%d:%s failed\n", taskNum, worker)
                    ps := schedPara{worker, taskNum, "failed"}
                    paraChan <- ps
                } else {
                    debug("%d:%s done\n", taskNum, worker)
                    ps := schedPara{worker, taskNum, "ok"}
                    paraChan <- ps
                }
            }(jobName, mapFiles[tasks[i]], phase, tasks[i], n_other, workers[i])
        }

        // Remove allocated tasks and workers from available tasks and workers
        workers = append(workers[:0], workers[i:]...)
        tasks   = append(tasks[:0], tasks[i:]...)
        debug("todo tasks: %v\n",tasks)
    }

	fmt.Printf("Schedule: %v phase done\n", phase)
}

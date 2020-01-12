package mapreduce

import (
	"fmt"
	"sync"
	
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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
	//Declaring a wait group for goroutines
	var wg sync.WaitGroup
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	
	
	for i:=0;i<ntasks;i++{	

		var doTaskArgs DoTaskArgs
		
		// Checking whether the request is for Map/Reduce
		if phase=="mapPhase"{
			doTaskArgs= DoTaskArgs{jobName,mapFiles[i],phase,i,n_other}
		}else{
			doTaskArgs= DoTaskArgs{JobName:jobName,Phase:phase,TaskNumber:i,NumOtherPhase:n_other}
		}
		//fmt.Println(doTaskArgs)
		//Adding entry to waitgroup
		wg.Add(1)
		
		//Calling a Goroutine for execute Writer
		go func(){			
			defer wg.Done()
			execute_writer(doTaskArgs,registerChan)
		 }()
		 
	}
	// fmt.Printf("Schedule: %v done\n", phase)
	
	
	wg.Wait()
	
	

	fmt.Printf("Schedule: %v done\n", phase)
	
 
}

func execute_writer(doTaskArgs DoTaskArgs, registerChan chan string){

	worker_add:=<-registerChan
			
			status:= call(worker_add, "Worker.DoTask",doTaskArgs, nil)
			if status {
				
				go func(){
					registerChan<-worker_add
				}()
				
			}else{
				execute_writer(doTaskArgs, registerChan)
			
			 }
}

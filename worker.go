package main

import "github.com/labstack/gommon/log"

var (
	MaxWorker = 1000
	MaxQueue  = 1000
)

type Payload struct {
}

// Job represents the job to be run
type Job struct {
	Payload Payload
}

func (p *Payload) UploadToS3() error {
	panic("need implementation")
}

// JobQueue A buffered channel that we can send work requests on
var JobQueue chan Job

// Worker represents the worker that executes the jobs
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request
				if err := job.Payload.UploadToS3(); err != nil {
					log.Errorf("")
				}
			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func NewQueue(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
	}
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool}
}

func (d *Dispatcher) Run() {
	// starting n number of queue
	for i := 0; i < MaxQueue; i++ {
		queue := NewQueue(d.WorkerPool)
		queue.Start()
	}

}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue: // push from queue to worker pool
			go func(job Job) {
				// try to obtain a worker job channel that is available
				// this will block until the channel is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

/**
 * Workerman -- Utility to run worker scripts based on job available in beanstalkd queue
 *
 * It looks into worker directory and subscribes for tubes by worker name.
 * When there is a job available, it spawns parallel process to execute worker related to tube.
 *
 * Command line arguments available:
 * --connect <addr:port> -- Beanstalkd server address and port to connect to. Default is 0.0.0.0:11300
 * --workers <path> -- Path to directory containing worker scripts
 */
package main

import (
	"bytes"
	"log"
	"flag"
	"github.com/nutrun/lentil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

/**	Address and port of Beanstalkd server */
var server = flag.String("connect", "0.0.0.0:11300", "Address:port of beanstalkd server. Default: 0.0.0.0:11300")

/** Directory containing executable workers */
var workersPath = flag.String("workers", "./workers/", "Directory path with worker scripts. Default: ./workers/")

var tubes []string

/** Interval in milliseconds between queue check */
var interval time.Duration = 100

/** Delay after queue error */
var errorDelay time.Duration = 1000

/** Delay after failed attempt to (re)connect to beanstalkd */
var reconnectDelay time.Duration = 5000

/** Worker runs counter */
var workerRuns uint64 = 1

/** Number of running workers for each tube */
var workersCount map[string]uint

/** Beanstalkd server connection */
var beanstalkd *lentil.Beanstalkd

/**
 * Process to run worker and collect output
 */
func workerRunner(channel chan string, instance uint64) {
	// Get worker name
	worker := <- channel
	workersCount[worker]++
	log.Printf("Starting worker %s:%d\n", worker, instance)
	var out bytes.Buffer
	cmd := exec.Command(worker, "")
	cmd.Stdout = &out
	error := cmd.Run()
	if error != nil {
		log.Printf("Worker %s:%d returned an error: %s", worker, instance, error)
	}
	log.Printf("Worker %s:%d output: %s", worker, instance, out.String())
	workersCount[worker]--
}

/**
 * Try to connect to beanstalkd until successfully connected
 */
func connect() *lentil.Beanstalkd {
	for {
		log.Printf("Connecting to %s", *server)
		beanstalkd, err := lentil.Dial(*server)
		if err != nil {
			log.Print(err)
			time.Sleep(reconnectDelay * time.Millisecond)
			continue
		}
		log.Printf("Connected to %s", *server)
		return beanstalkd
	}
}

/**
 * Subscribe to queue tubes
 */
func subscribe(tubes []string) {
	for i, _ := range tubes {
		_, err := beanstalkd.Watch(tubes[i])
		if err != nil {
			log.Print(err)
		} else {
			log.Printf("Subscribed for %s\n", tubes[i])
		}
	}
}

/**
 * Looks for workers in specified directory
 */
func listWorkers() []string {
	tubes, err := filepath.Glob("*")
	if err != nil {
		log.Fatal(err)
	}
	return tubes
}

/**
 * Watches for changes in workers, and subscribes on the fly
 */
func watcher() {
	workers, _ := filepath.Glob("*")
	watchedTubes, _ := beanstalkd.ListTubesWatched()
	mWatched := make(map[string]bool)
	for _, watchedTube := range watchedTubes {
		if watchedTube != "default" {
			mWatched[watchedTube] = true
		}
	}
	mWorkers := make(map[string]bool)
	for _, worker := range workers {
		mWorkers[worker] = true
	}
	for worker, _ := range mWorkers {
		// Check if we need to subscribe to new tube
		if _, has := mWatched[worker]; !has {
			log.Printf("Subscribing to %s", worker)
			beanstalkd.Watch(worker)
			workersCount[worker] = 0
		}
	}
	for tube, _ := range mWatched {
		// Check if we no longer have worker
		if _, has := mWorkers[tube]; !has {
			log.Printf("Ignoring %s", tube)
			beanstalkd.Ignore(tube)
			delete(workersCount, tube)
		}
	}
	tubes = workers
}

/**
 * Main entry point
 */
func main() {
	// Parse command line arguments
	flag.Parse()

	// Connect to beanstalkd
	beanstalkd = connect()

	// Create map for running worker counts
	workersCount = make(map[string]uint)

	// Go to workers dir
	err := os.Chdir(*workersPath)
	if err != nil {
		log.Fatal(err)
	}

	// Subscribe to queues
	watcher()

	// Create communications channel
	channel := make(chan string)

	// Wait for jobs
	for {
		job, err := beanstalkd.ReserveWithTimeout(1)
		if err != nil {
			var errType string = err.Error()
			// Time out error is ok
			if !strings.Contains(errType, "TIMED_OUT") {
				log.Printf("Error reserving a job: %s", errType)
				beanstalkd.Quit()
				beanstalkd = connect()
				subscribe(tubes)
			}
		} else {
			// Got job, get tube name
			stats, err := beanstalkd.StatsJob(job.Id)
			if err != nil {
				log.Printf("Error retrieving job %d stats: %s", job.Id, err)
				time.Sleep(errorDelay * time.Millisecond)
				continue
			}
			var worker string = stats["tube"]
			// Put job back to queue
			err = beanstalkd.Release(job.Id, 0, 0)
			if err != nil {
				log.Printf("Error releasing job %d: %s", job.Id, err)
				time.Sleep(errorDelay * time.Millisecond)
				continue
			}
			// Run independent worker
			go workerRunner(channel, workerRuns)
			// Send worker the name of the tube to process
			channel <- worker
			// See how many workers we have running at the moment
			log.Printf("Running %s workers count: %d\n", worker, workersCount[worker])
		}
		if workerRuns % 5 == 0 {
			// Run watcher each 5th tick
			watcher()
		}
		// Be polite to system
		time.Sleep(interval * time.Millisecond)
		workerRuns++
	}
}

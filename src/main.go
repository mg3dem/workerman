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
	"time"
)

/**	Address and port of Beanstalkd server */
var server = flag.String("connect", "0.0.0.0:11300", "Address:port of beanstalkd server. Default: 0.0.0.0:11300")

/** Directory containing executable workers */
var workersPath = flag.String("workers", "./workers/", "Directory path with worker scripts. Default: ./workers/")

/** Interval in milliseconds between queue check */
var interval time.Duration = 100

/** Delay after queue error */
var errorDelay time.Duration = 1000

/** Worker runs counter */
var workerRuns uint64 = 1

/** Number of running workers */
var workersCount uint = 0

/**
 * Process to run worker and collect output
 */
func workerRunner(channel chan string, instance uint64) {
	// Get worker name
	worker := <-channel
	workersCount++
	log.Printf("Starting worker %s:%d\n", worker, instance)
	var out bytes.Buffer
	cmd := exec.Command(worker, "")
	cmd.Stdout = &out
	error := cmd.Run()
	if error != nil {
		log.Printf("Worker %s:%d returned an error: %s", worker, instance, error)
	}
	log.Printf("Worker %s:%d output: %s", worker, instance, out.String())
	workersCount--
}

/**
 * Main entry point
 */
func main() {
	// Parse command line arguments
	flag.Parse()

	// Connect to beanstalkd
	log.Printf("Connecting to %s", *server)
	beanstalkd, error := lentil.Dial(*server)
	if error != nil {
		log.Fatal(error)
	}

	// Get list of available worker scripts
	log.Printf("Workers directory: %s", *workersPath)
	error = os.Chdir(*workersPath)
	if error != nil {
		log.Fatal(error)
	}
	workerFiles, error := filepath.Glob("*")
	if error != nil {
		log.Fatal(error)
	}

	// Subscribe to tubes. TubeName == WorkerName
	for worker := range workerFiles {
		_, error := beanstalkd.Watch(workerFiles[worker])
		if error != nil {
			log.Fatal(error)
		} else {
			log.Printf("Subscribed for %s\n", workerFiles[worker])
		}
	}

	// Create communications channel
	channel := make(chan string)

	// Wait for jobs
	for true {
		job, error := beanstalkd.Reserve()
		if error != nil {
			log.Printf("Error reserving a job: %s", error)
			time.Sleep(errorDelay * time.Millisecond)
			continue
		} else {
			// Got job, get tube name
			stats, error := beanstalkd.StatsJob(job.Id)
			if error != nil {
				log.Printf("Error retrieving job %d stats: %s", job.Id, error)
				time.Sleep(errorDelay * time.Millisecond)
				continue
			}
			var worker string = stats["tube"]
			// Put job back to queue
			error = beanstalkd.Release(job.Id, 0, 0)
			if error != nil {
				log.Printf("Error releasing job %d: %s", job.Id, error)
				time.Sleep(errorDelay * time.Millisecond)
				continue
			}
			// Run independent worker
			go workerRunner(channel, workerRuns)
			// Send worker the name of the tube to process
			channel <- worker
			// See how many workers we have running at the moment
			log.Printf("Running workers count: %d\n", workersCount)
		}
		// Be polite to system
		time.Sleep(interval * time.Millisecond)
		workerRuns++
	}
}

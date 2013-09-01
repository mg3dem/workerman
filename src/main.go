/**
 * Workerman -- Utility to run worker scripts based on job available in beanstalkd queue
 *
 * It looks into worker directory and subscribes for tubes by worker name.
 * When there is a job available, it spawns parallel process to execute worker related to tube.
 *
 * Command line arguments available:
 * --connect <addr:port> -- Beanstalkd server address and port to connect to. Default is 0.0.0.0:11300
 * --workers <path> -- Path to directory containing worker scripts
 * --user username -- User name to switch account. Works only if run as root.
 *
 * @author Dmitry Vovk <dmitry.vovk@gmail.com>
 * @package Марк Абрамович Воркерман
 *
 */
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/kr/beanstalk"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type WorkerCommand struct {
	Command string
	Options map[string]string
}

type Limits struct {
	Total  uint
	Min    uint
	Queues map[string]uint
}

type Stats struct {
	TotalRuns       uint64 // Workers total runs counter
	TotalCycles     uint64 // Number of cycles
	TotalRecoveries uint64 // Number of job reserve error recoveries
	LastError       string
	Runs            map[string]uint64 // Count runs for each worker
	Errors          map[string]uint64 // Worker errors count (non zero return codes)
	Running         map[string]uint   // Now running count
	TotalRunning    uint
	Limits          *Limits
}

type Sync struct {
	Worker string
	Count int8
	Error bool
}

type Queue struct {
	conn *beanstalk.Conn
	tube *beanstalk.Tube
}

var (
	/** Address and port of Beanstalkd server */
	server = flag.String("connect", "0.0.0.0:11300", "Address:port of beanstalkd server. Default: 0.0.0.0:11300")

	/** Directory containing executable workers */
	workersPath = flag.String("workers", "workers", "Directory path with worker scripts. Default: workers")

	runAs = flag.String("user", "", "Specify user account name to use")

	myDir string
	cfgPath string

	/** Interval in milliseconds between queue check */
	interval time.Duration = 10

	/** Delay after failed attempt to (re)connect to beanstalkd */
	reconnectDelay time.Duration = 5000

	/** Control tube connection */
	commandConn *beanstalk.Conn

	/** Tubes connections */
	connections map[string]Queue

	commandTubeName, responseTubeName string

	responseTube *beanstalk.Tube

	commandTube *beanstalk.TubeSet

	limits Limits

	stats Stats

	statsChannel chan Sync
)

const (
	INPUT_PREFIX        = "Worker-to."
	OUTPUT_PREFIX       = "Worker-from."
	DEFAULT_QUEUE_LIMIT = 5
	WORKERS_MAX         = 100 // Maximum number of workers to run
	WORKERS_MIN         = 5   // Minimal number of workers to allow
)

func (l *Limits) Json() ([]byte, error) {
	return json.Marshal(l)
}

func (l *Limits) PrettyJson() ([]byte, error) {
	return json.MarshalIndent(l, "", "  ")
}

/**
 * Process to run worker and collect output
 */
func workerRunner(worker string) {
	var hasError bool = false
	statsChannel <- Sync{Worker: worker, Count: 1, Error: hasError}
	log.Printf("Starting %s:%d\n", worker, stats.Runs[worker])
	var out bytes.Buffer
	cmd := exec.Command("./"+worker, "")
	cmd.Stdout = &out
	error := cmd.Run()
	if error != nil {
		if strings.Contains(error.Error(), "no such file") {
			// Worker file is removed, unsubscribe
			delete(connections, worker)
			log.Printf("Unsubscribed %s", worker)
		} else {
			hasError = true
			log.Printf("Worker %s:%d returned an error: %s", worker, stats.Runs[worker], error)
		}
	}
	// Log output if any
	if out.Len() > 0 {
		log.Printf("Worker %s:%d output: %s", worker, stats.Runs[worker], out.String())
	}
	statsChannel <- Sync{Worker: worker, Count: -1, Error: hasError}
}

/**
 * Try to connect to beanstalkd until successfully connected
 */
func connect() *beanstalk.Conn {
	for {
		log.Printf("Connecting to %s...", *server)
		beanstalk, err := beanstalk.Dial("tcp", *server)
		if err != nil {
			log.Printf("Could not connect: %v", err)
			time.Sleep(reconnectDelay * time.Millisecond)
			continue
		}
		log.Printf("Connected!")
		return beanstalk
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
	// Collect available workers
	workerFiles := listWorkers()
	newWorkerFiles := make(map[string]bool)
	for _, worker := range workerFiles {
		newWorkerFiles[worker] = true
	}
	// Check if we have subscribed already
	for _, tube := range workerFiles {
		// No, we have not
		if _, ok := connections[tube]; !ok {
			conn := connect()
			connections[tube] = Queue{conn, &beanstalk.Tube{conn, tube}}
			// No previous worker runs, add counters
			if _, ok := stats.Runs[tube]; !ok {
				stats.Runs[tube] = 0
			}
			if _, ok := stats.Running[tube]; !ok {
				stats.Running[tube] = 0
			}
			if _, ok := limits.Queues[tube]; !ok {
				limits.Queues[tube] = DEFAULT_QUEUE_LIMIT
			}
			log.Printf("Subscribed to %s", tube)
		}
	}
	// Check if we need to unsubscribe
	for tube, _ := range connections {
		if _, ok := newWorkerFiles[tube]; !ok {
			delete(connections, tube)
			delete(stats.Running, tube)
			log.Printf("Unsubscribed %s", tube)
		}
	}
}

/**
 * Process command received
 */
func processCommand(cmd WorkerCommand) {
	var payload []byte
	switch cmd.Command {
	default:
		log.Printf("Unknown or unsupported command: %s", cmd.Command)
		return
	case "getLimits":
		payload = getLimits()
	case "getStatus":
		payload = getStatus()
	case "setLimits":
		payload = setLimits(cmd.Options)
		writeConfig()
	}
	if payload != nil {
		responseTube.Put(payload, 0, 0, 5)
	}
}

/**
 * Returns JSON encoded current limit settings
 */
func getLimits() []byte {
	response, err := limits.Json()
	if err != nil {
		log.Printf("Could not encode limits: %v", err)
		return nil
	}
	return response
}

/**
 * Returns JSON encoded statistics
 */
func getStatus() []byte {
	response, err := json.Marshal(stats)
	if err != nil {
		log.Printf("Could not encode status: %v", err)
		return nil
	}
	return response
}

/**
 * Process setLimits command
 */
func setLimits(options map[string]string) []byte {
	for key, value := range options {
		if _, has := limits.Queues[key]; has {
			intLimit, err := strconv.Atoi(value)
			if err == nil {
				limits.Queues[key] = uint(intLimit)
				log.Printf("Setting %s => %s", key, value)
			}
		} else if key == "*" {
			intLimit, err := strconv.Atoi(value)
			if err == nil {
				limits.Total = uint(intLimit)
				log.Printf("Setting total limit to %s", value)
			}
		} else if key == "-" {
			intLimit, err := strconv.Atoi(value)
			if err == nil {
				limits.Min = uint(intLimit)
				log.Printf("Setting minimum workers to %s", value)
			}
		} else {
			log.Printf("Skipping '%s', not subscribed", key)
		}
	}
	return getStatus()
}

/**
 * Checks if worker can be run
 */
func canRunWorker(worker string) bool {
	// Always run at least limits.Min workers
	if stats.Running[worker] < limits.Min {
		return true
	}
	// See if total limit allows
	if stats.TotalRunning < limits.Total {
		// Do we have limit set for the worker?
		if limit, has := limits.Queues[worker]; !has {
			return true
		} else {
			return limit > stats.Running[worker]
		}
	}
	return false
}

func readConfig() {
	file, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		log.Printf("Notice: could not read config file: %s", err)
		return
	}
	var tempLimits Limits
	jsErr := json.Unmarshal(file, &tempLimits)
	if jsErr != nil {
		log.Printf("Warning: could not parse config file: %s", jsErr)
		return
	}
	limits = tempLimits
	log.Printf("Loaded config: %s", getLimits())
}

func writeConfig() {
	log.Printf("Writing out config file %s", cfgPath)
	cfg, encErr := limits.PrettyJson()
	if encErr != nil {
		log.Printf("Error encoding limits: %v", encErr)
		return
	}
	writeErr := ioutil.WriteFile(cfgPath, cfg, 0600)
	if writeErr != nil {
		log.Printf("Error writing config %s: %v", cfgPath, writeErr)
	}
}

/**
 * Switch user account if needed
 */
func switchUser() {
	if userAccount, uErr := user.Current(); uErr != nil {
		log.Fatalf("Fatal error: could not get current user: %v", uErr)
	} else {
		if userAccount.Uid == "0" {
			if *runAs != "" {
				if runAsUser, lErr := user.Lookup(*runAs); lErr == nil {
					uid, _ := strconv.Atoi(runAsUser.Uid)
					sErr := syscall.Setuid(uid)
					if sErr != nil {
						log.Fatalf("Fatal error: could not switch to user %s: %v", *runAs, sErr)
					}
					log.Printf("Switched to run as user '%s'", runAsUser.Username)
				} else {
					log.Fatalf("Fatal error: user '%s' not found", *runAs)
				}
			} else {
				log.Printf("Warning: running as root!")
			}
		} else {
			if *runAs != "" {
				log.Printf("Warning: asked to run as user '%s', but cannot switch when run as '%s'", *runAs, userAccount.Username)
			} else {
				log.Printf("Running as user '%s'", userAccount.Username)
			}
		}
	}
}

/**
 * Collect stats from running goroutines
 */
func statisticsCollector() {
	var m Sync
	for {
		m = <- statsChannel
		if _, has := stats.Runs[m.Worker]; has {
			if m.Error {
				stats.Errors[m.Worker]++
			}
			if m.Count == 1 {
				stats.TotalRuns += 1
				stats.Runs[m.Worker] += 1
				stats.Running[m.Worker] += 1
				stats.TotalRunning += 1
			} else {
				stats.Running[m.Worker] -= 1
				stats.TotalRunning -= 1
			}
		} else {
			log.Printf("Do not have %s in stats", m.Worker)
		}
	}
}

/**
 * Main entry point
 */
func main() {
	// Use all available CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())
	// Parse command line arguments
	flag.Parse()
	switchUser()
	_myDir, wErr := os.Getwd()
	if wErr != nil {
		log.Fatalf("Error getting current working directory: %v", wErr)
	}
	myDir = _myDir
	cfgPath = os.Args[0] + ".json"
	// Get hostname
	hostName, errHost := os.Hostname()
	if errHost != nil {
		log.Fatalf("Error getting host name: %v", errHost)
	}
	log.Printf("Hostname is '%s'", hostName)
	commandTubeName = INPUT_PREFIX + hostName
	responseTubeName = OUTPUT_PREFIX + hostName
	statsChannel = make(chan Sync)
	// Create worker command queue connection
	commandConn = connect()
	// Create map for running worker counts
	stats.Running = make(map[string]uint)
	stats.Runs = make(map[string]uint64)
	stats.Errors = make(map[string]uint64)
	stats.Limits = &limits
	limits.Total = WORKERS_MAX
	limits.Min = WORKERS_MIN
	limits.Queues = make(map[string]uint)
	// Pick up previous settings if exist
	readConfig()
	// Go to workers dir
	errDir := os.Chdir(*workersPath)
	if errDir != nil {
		log.Fatalf("Error changing to workers directory: %v", errDir)
	}
	// Prepare connection pool
	connections = make(map[string]Queue)
	// Create response tube
	responseTube = &beanstalk.Tube{commandConn, responseTubeName}
	// Prepare command tube
	commandTube = &beanstalk.TubeSet{commandConn, make(map[string]bool)}
	commandTube.Name[commandTubeName] = true
	commandTube.Name["default"] = false
	log.Printf("Subscribed to command queue %s", commandTubeName)
	go statisticsCollector()
	// Wait for jobs. No fatals behind this point!
	for {
		// Check for available workers once in a while
		if stats.TotalCycles%5 == 0 {
			watcher()
		}
		// Is there command available?
		id, body, errCommandReserve := commandTube.Reserve(0)
		// Process command
		if errCommandReserve == nil {
			commandConn.Delete(id)
			var cmd WorkerCommand
			errDecode := json.Unmarshal(body, &cmd)
			if errDecode == nil {
				go processCommand(cmd)
			} else {
				log.Printf("Could not parse command: %v", body)
			}
		} else {
			// Timeout error is ok, other is not
			if !strings.Contains(errCommandReserve.Error(), "timeout") {
				log.Printf("Command error: %v", errCommandReserve)
			}
		}
		// Loop over queues
		for worker, conn := range connections {
			// Only read stats if worker can be run
			if canRunWorker(worker) {
				tubeStats, errStats := conn.tube.Stats()
				if errStats == nil {
					// ... and when there are jobs
					readyJobsCount, _ := strconv.Atoi(tubeStats["current-jobs-ready"])
					if readyJobsCount > 0 {
						go workerRunner(worker)
					}
				}
			}
		}
		stats.TotalCycles++
		// Be polite to system
		time.Sleep(interval * time.Millisecond)
	}
}

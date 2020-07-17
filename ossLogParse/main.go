package main

import (
	"os"
	"syscall"
	"fmt"
	"strings"
	"flag"
	"log"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"
	"crypto/md5"
)

typedef AccountSS {	/* Account Status Statistics  */
	Method		string	/* GET, PUT, DELETE, POST, HEAD, OPTIONS ... etc */
	StatusCode	string	/* 200, 201, 204, 401, 403, 404, 499, 500, 501, 502 ... etc */
	Client		[]string	/* http client name slice, some account may have serval http client */
	Count		int64	
	AvgTime		int64	/* Average response time, in ms */
	MaxTime		int64
	MinTime		int64
	AvgSize		int64
	MaxSize		int64
	MinSize		int64
	TotalSize	int64
}

type AccountLogValue struct {
	AccountName	string
	Ass		AcountSS
}

type AccountLogKey struct {
	AccountName	string
	timeHMS		string		/* hour: 08:00:00-08:59:59, minute: __:08:00-__:08:59,  second: __:__:00-__:__:00 */
}

const (
	WORKERS = 16
	READBUFFER = 1024 * 1024 
)

var workers	int
// output result file, default '/tmp/AccountLogParse.log'
var outputFile	string
var logger	*log.Logger
var verbose	int
var readBuffer	int
var accountSSMap = map[AccountLogKey]AccountLogValue{}
var accountSSMapMutex	sync.Mutex

/*
func parseConfigFile(exFrom string, dMap, fMap map[string]bool) error {
	exBytes, exErr := ioutil.ReadFile(exFrom)
	if exErr != nil {
		// do nothing
		return exErr
	} else {
		exStrings := strings.Split(string(exBytes), "\n")
		for _, s := range exStrings {
			if strings.HasPrefix(s, "#") { 
				continue
			}
			if strings.HasSuffix(s, "/") {
				s = s[:len(s)-1]
				if len(s) > 0 {
					dMap[s] = true
				}
			} else {
				if len(s) > 0 {
					fMap[s] = true
				}
			}
		}
	}
	return exErr
}
*/

func comma(s string) string {
        n := len(s)
        if n <= 3 {
                return s
        }
        return comma(s[:n-3]) + "," + s[n-3:]
}

func V(number int64) string {
	return comma(fmt.Sprintf("%d", number))
}

func parseLogFile()


func main() {
	flag.IntVar(&workers, "workers", WORKERS, "concurrent goroutine workers")
	flag.StringVar(&configFile, "config", "account_log_parse.conf", "account log parse config file")
	flag.StringVar(&output, "output", "/tmp/account_log_result", "account log parse result file")

        flag.Parse()
        args := flag.Args()
        if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] OutputFile\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

        l, err := os.OpenFile(outputFile, os.O_APPEND | os.O_RDWR | os.O_CREATE, 0755)
        if err != nil {
                fmt.Fprintf(os.Stderr, "os.OpenFile('%s') error: %v", outputFile, err)
		os.Exit(2)
        }
        defer l.Close()
	logger = log.New(l, "", log.LstdFlags)

	/*
	if err = parseConfigFile(excludeFrom, excludeDirMap, excludeFileMap); err != nil {
		//do nothing
	}
	*/

	file, err = os.Open(args[1]) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()


	workersSema := make(chan struct{}, workers)
        var nWorkers sync.WaitGroup

	nWorkers.Add(1)
       	workersSema <- struct{}{}

	go parseLogFile(file, workersSema)
	go parseLogFile(file, mapChan, workersSema)

        go func() {
                nWorkers.Wait()
                close(dfPairChan)
        }()
}

package main

import (
	"os"
	"fmt"
	"strings"
	"flag"
	"log"
	"io"
	//"io/ioutil"
	//"path/filepath"
	"sync"
	"bufio"
)

type AccountSS struct {	/* Account Status Statistics  */
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
	Ass		AccountSS
}

type AccountLogKey struct {
	AccountName	string
	srcIp		string
	timeHMS		string		/* hour: 08:00:00-08:59:59, minute: __:08:00-__:08:59,  second: __:__:00-__:__:00 */
}

const (
	WORKERS = 16
	LINESOFPERWORKER = 4096
)

const ( 
	LOGDate = iota
	LOGHostname
	LOGServerName
	LOGHttpMethod
	LOGUrl
	LOGHttpVersion
	LOGHttpStatus
	LOGResponseTime
	LOGHttpSize
	LOGNoop1
	LOGNoop2
	LOGHttpClient
	LOGSrcAddr
	LOGNoop3
	LOGDstAddr
)



var logDateIndex 		int = LOGDate
var logHostnameIndex		int = LOGHostname
var logServerNameIndex		int = LOGServerName
var logHttpMethodIndex		int = LOGHttpMethod
var logUrlIndex			int = LOGUrl
var logHttpStatusIndex		int = LOGHttpStatus
var logHttpResponseTimeIndex	int = LOGResponseTime
var logHttpSizeIndex		int = LOGHttpSize
var logHttpClientIndex		int = LOGHttpClient
var logSrcAddr			int = LOGSrcAddr


var workers	int
// output result file, default "/tmp/AccountLogParseResult"
var outputFile	string
var logger	*log.Logger
var verbose	int
var readBuffer	int
var accountSSMap = map[AccountLogKey]AccountLogValue{}
var accountSSMapMutex	sync.Mutex
var configFile	string

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

func logReadLine(br *bufio.Reader) (string, error) {
	line, e := br.ReadString('\n')
	if e == nil {
		if len(line) > 0 {
			return line[:len(line)-1], nil
		} else {
			return "", nil
		}
	}
	if e == io.EOF {
		if len(line) > 0 && line[len(line)-1:] == "\n" {
			return line[:len(line)-1], io.EOF
		} else {
			return "", io.EOF
		}
	}
	return "", io.EOF
}

func parseLogFile(br *bufio.Reader, nWorkers *sync.WaitGroup, mapChan chan<- map[AccountLogKey]AccountLogValue, workersSema chan struct{}) {
	defer nWorkers.Done()
	defer func() { <-workersSema }()

	var lines = make([]string, 0)
	for {
		line, err := logReadLine(br)
		if len(line) > 0 {
			lines = append(lines, line)
		}
		if len(lines) > LINESOFPERWORKER {
			nWorkers.Add(1)
			workersSema <- struct{}{}
			go parseLines(lines[:LINESOFPERWORKER], nWorkers, mapChan, workersSema)
			lines = lines[LINESOFPERWORKER:]
		}
		if err == io.EOF {
			if len(lines) > 0 {
				nWorkers.Add(1)
				workersSema <- struct{}{}
				go parseLines(lines, nWorkers, mapChan, workersSema)
			}
			break
		}
	}
}

func parseLines(lines []string, nWorkers *sync.WaitGroup, mapChan chan<- map[AccountLogKey]AccountLogValue, workersSema chan struct{}) {
	defer nWorkers.Done()
	defer func() { <-workersSema }()

	accountMap := map[AccountLogKey]AccountLogValue{}
	for _, line := range lines {
		var k AccountLogKey
		var v AccountLogValue
		words := strings.Split(line, " ")
		if !strings.Contains(words[LOGUrl], "AUTH_") {
			continue
		}
		k.AccountName = getAccountFromUrl(words[LOGUrl])
		fmt.Printf("%s\n", k.AccountName)
		accountMap[k] = v
	}
}

func getAccountFromUrl(url string) string {
	words := strings.Split(url, "/")
	return words[2]
}

func getHourFromDate(date string) string {
	//words := strings.Split(date, )
	return ""
}


func main() {
	flag.IntVar(&workers, "workers", WORKERS, "concurrent goroutine workers")
	flag.StringVar(&configFile, "config", "AccountLogParse.conf", "account log parse config file")
	flag.StringVar(&outputFile, "output", "/tmp/AccountLogParseResult", "account log parse result file")

        flag.Parse()
        args := flag.Args()
        if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] LogFile\n", os.Args[0])
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

	
	file, err := os.Open(args[0]) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	br := bufio.NewReader(file)

	workersSema := make(chan struct{}, workers)
        var nWorkers sync.WaitGroup

	nWorkers.Add(1)
       	workersSema <- struct{}{}
	mapChan := make(chan map[AccountLogKey]AccountLogValue, workers/2)

	parseLogFile(br, &nWorkers, mapChan, workersSema)

        go func() {
                nWorkers.Wait()
                close(mapChan)
        }()

        for v:= range mapChan {
		fmt.Println(v)
	}
	
	fmt.Printf("\t Finish parse\n")

}

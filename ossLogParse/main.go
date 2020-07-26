package main

import (
	"os"
	"fmt"
	"strings"
	"strconv"
	"flag"
	"log"
	"io"
	//"io/ioutil"
	//"path/filepath"
	"sync"
	"bufio"
)

type AccountLogValue struct {	/* Account Status Statistics  */
	Count		int64	
	MaxTime		float64
	MinTime		float64
	TotalTime	float64
	AverageTime	float64
	MaxSize		int64
	MinSize		int64
	TotalSize	int64
	AverageSize	int64
}

type AccountLogKey struct {
	AccountName	string
	TimeHMS		string	/* hour: 08:00:00-08:59:59, minute: __:08:00-__:08:59,  second: __:__:00-__:__:00 */
	Method		string	/* GET, PUT, DELETE, POST, HEAD, OPTIONS ... etc */
	StatusCode	string	/* 200, 201, 204, 401, 403, 404, 499, 500, 501, 502 ... etc */
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
)



var logDateIndex 		int = LOGDate
var logHostnameIndex		int = LOGHostname
var logServerNameIndex		int = LOGServerName
var logHttpMethodIndex		int = LOGHttpMethod
var logUrlIndex			int = LOGUrl
var logHttpStatusIndex		int = LOGHttpStatus
var logHttpResponseTimeIndex	int = LOGResponseTime
var logHttpSizeIndex		int = LOGHttpSize


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

	var kh, km, ks 	AccountLogKey
	var time float64
	var size int64
	var timeErr, sizeErr bool
	var err error
	var TimeSizeErr, TotalLines int64
	for _, line := range lines {
		TotalLines++	
		words := strings.Split(line, " ")
		if !strings.Contains(words[LOGUrl], "AUTH_") {
			continue
		}
		if time, err = strconv.ParseFloat(words[LOGResponseTime], 32); err != nil {
			timeErr = true
		}
		if size, err = strconv.ParseInt(words[LOGHttpSize], 10, 32); err != nil {
			sizeErr = true
		}
		if timeErr || sizeErr {
			TimeSizeErr++
			continue
		}

		//if strings.Contains(line, "AUTH_GIS-ASS-LPS-PRD-DR") {
		//	fmt.Println(line)
		//}

		kh.AccountName = getAccountFromUrl(words[LOGUrl])
		kh.TimeHMS = getHourFromDate(words[LOGDate])
		kh.Method = words[LOGHttpMethod]
		kh.StatusCode = words[LOGHttpStatus]
		
		if vh, ok := accountMap[kh]; ok {
			vh.TotalTime += time
			vh.TotalSize += size
			if time > vh.MaxTime {
				vh.MaxTime = time
			}
			if time < vh.MinTime {
				vh.MinTime = time
			}
			if size > vh.MaxSize {
				vh.MaxSize = size
			}
			if size < vh.MinSize {
				vh.MinSize = size
			}
			vh.Count++
			accountMap[kh] = vh
		} else {
			vh.TotalTime = time
			vh.TotalSize = size
			vh.MaxTime = time
			vh.MinTime = time
			vh.MaxSize = size
			vh.MinSize = size
			vh.Count = 1
			accountMap[kh] = vh
		}
		//if kh.AccountName == "AUTH_GIS-ASS-LPS-PRD-DR" {
		//	fmt.Println(kh.AccountName, accountMap[kh].Count)
		//}

		km.AccountName = getAccountFromUrl(words[LOGUrl])
		km.TimeHMS = getHourMinuteFromDate(words[LOGDate])
		km.Method = words[LOGHttpMethod]
		km.StatusCode = words[LOGHttpStatus]
		if vm, ok := accountMap[km]; ok {
			vm.TotalTime += time
			vm.TotalSize += size
			if time > vm.MaxTime {
				vm.MaxTime = time
			}
			if time < vm.MinTime {
				vm.MinTime = time
			}
			if size > vm.MaxSize {
				vm.MaxSize = size
			}
			if size < vm.MinSize {
				vm.MinSize = size
			}
			vm.Count++
			accountMap[km] = vm
                } else {
                        vm.TotalTime = time
                        vm.TotalSize = size
                        vm.MaxTime = time
                        vm.MinTime = time
                        vm.MaxSize = size
                        vm.MinSize = size
                        vm.Count = 1
			accountMap[km] = vm
                }

		ks.AccountName = getAccountFromUrl(words[LOGUrl])
		ks.TimeHMS = getHourMinuteSecondFromDate(words[LOGDate])
		ks.Method = words[LOGHttpMethod]
		ks.StatusCode = words[LOGHttpStatus]
		if vs, ok := accountMap[ks]; ok {
			vs.TotalTime += time
			vs.TotalSize += size
			if time > vs.MaxTime {
				vs.MaxTime = time
			}
			if time < vs.MinTime {
				vs.MinTime = time
			}
			if size > vs.MaxSize {
				vs.MaxSize = size
			}
			if size < vs.MinSize {
				vs.MinSize = size
			}
			vs.Count++
			accountMap[ks] = vs
                } else {
                        vs.TotalTime = time
                        vs.TotalSize = size
                        vs.MaxTime = time
                        vs.MinTime = time
                        vs.MaxSize = size
                        vs.MinSize = size
                        vs.Count = 1
			accountMap[ks] = vs
                }
		//if kh.AccountName == "AUTH_GIS-ASS-LPS-PRD-DR" {
		//	fmt.Printf("TotalLines: %d, Time: %f\n", TotalLines, accountMap[kh].TotalTime)
		//}
	}
	mapChan <- accountMap
}

func getAccountFromUrl(url string) string {
	words := strings.Split(url, "/")
	index := strings.Index(words[2], "?")

	if index > 0 {
		return words[2][:index]
	}
	return words[2]
}

func getHourFromDate(date string) string {
	words := strings.Split(date, ":")
	tmp := words[0]
	tmp = tmp[len(tmp)-2:]
	return tmp + ":00:00-" + tmp + ":59:59"
}

func getHourMinuteFromDate(date string) string {
	words := strings.Split(date, ":")
	tmp := words[0] + ":" + words[1]
	tmp = tmp[len(tmp)-5:]
	return tmp + ":00-" + tmp + ":59"
}

func getHourMinuteSecondFromDate(date string) string {
	words := strings.Split(date, ":")
	tmp := words[0] + ":" + words[1] + ":" + words[2][:2]
	tmp = tmp[len(tmp)-8:]
	return tmp + "-" + tmp
}

func summaryAccountMap(sum, item map[AccountLogKey]AccountLogValue) {
	for itemk, itemv := range item {
		if sumv, ok := sum[itemk]; ok {
			sumv.Count += itemv.Count
			sumv.TotalSize += itemv.TotalSize
			sumv.TotalTime += itemv.TotalTime
			if itemv.MaxSize > sumv.MaxSize {
				sumv.MaxSize = itemv.MaxSize
			}
			if itemv.MinSize < sumv.MinSize {
				sumv.MinSize = itemv.MinSize
			}
			if itemv.MaxTime > sumv.MaxTime {
				sumv.MaxTime = itemv.MaxTime
			}
			if itemv.MinTime < sumv.MinTime {
				sumv.MinTime = itemv.MinTime
			}
			sum[itemk] = sumv
		} else {
			sum[itemk] = itemv
		}
	}
	for sumk, sumv := range sum {
		sumv.AverageSize = sumv.TotalSize/sumv.Count
		sumv.AverageTime = sumv.TotalTime/float64(sumv.Count)
		sum[sumk] = sumv
	}
}

func main() {
	flag.IntVar(&workers, "workers", WORKERS, "concurrent goroutine workers")
	flag.StringVar(&configFile, "config", "LogParse.conf", "account log parse config file")
	flag.StringVar(&outputFile, "output", "/tmp/LogParseResult", "account log parse result file")

        flag.Parse()
        args := flag.Args()
        if len(args) < 1 || len(args) > 3 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] LogFile [AccountName] [TimeField]\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

	var logFile, accountName, timeField	string
	if len(args) == 1 {
		logFile = args[0]
	} else if len(args) == 2 {
		logFile = args[0]
		accountName = args[1]
	} else if len(args) == 3 {
		logFile = args[0]
		accountName = args[1]
		timeField = args[2]
	}

        l, err := os.OpenFile(outputFile, os.O_TRUNC | os.O_RDWR | os.O_CREATE, 0755)
        if err != nil {
                fmt.Fprintf(os.Stderr, "os.OpenFile('%s') error: %v", outputFile, err)
		os.Exit(2)
        }
        defer l.Close()
	logger = log.New(l, "", 0)

	/*
	if err = parseConfigFile(excludeFrom, excludeDirMap, excludeFileMap); err != nil {
		//do nothing
	}
	*/
	
	file, err := os.Open(logFile) // For read access.
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

	var TotalResultMap = make(map[AccountLogKey]AccountLogValue)
        for m:= range mapChan {
		summaryAccountMap(TotalResultMap, m)
	}

	fmt.Println(timeField)	
	for ik, iv := range TotalResultMap {
		if strings.Contains(ik.AccountName, accountName) {
			logger.Printf("%v: \n", ik)
			logger.Printf("\t\t Count: %d\n", iv.Count) 
			logger.Printf("\t\t MaxTime: %.4f\n", iv.MaxTime) 
			logger.Printf("\t\t MinTime: %.4f\n", iv.MinTime) 
			logger.Printf("\t\t AverageTime: %.4f\n", iv.AverageTime) 
			logger.Printf("\t\t MaxSize: %d\n", iv.MaxSize) 
			logger.Printf("\t\t MinSize: %d\n", iv.MinSize) 
			logger.Printf("\t\t TotalSize: %d\n", iv.TotalSize) 
			logger.Printf("\t\t AverageSize: %d\n", iv.AverageSize) 
		}
	}
}

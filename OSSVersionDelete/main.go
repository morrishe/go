package main

import (
	"os"
	"fmt"
	"strings"
	"strconv"
	"flag"
	"log"
	"io"
	"path/filepath"
	"sync"
	"time"
	"crypto/md5"
)

type DirInfo struct {
        srcDir          string
	fileCount	int64
	deleteFileCount	int64
	dirCount	int64
	totalSize	int64
	totalDeleteSize	int64
	errCount	int64
	toBeContinue	bool
}

type FileNode struct {
        srcFile		string
        srcSize		int64
	err             bool
        deleteSize	int64
}


const (
	DIRWORKERS = 1024
	FILEWORKERS = 4096
	READDIRCOUNT = 4096
	VERSIONHOURS = 72
)

var dirWorkers	int
var fileWorkers	int
var versionHours	int64
// log file, default '/tmp/OSSVersionDelete.log'
var logfile	string
var logger	*log.Logger
var verbose	int
var readdirCount	int
var euid = os.Geteuid()

/* when a big directory is encounted, we need invoke *File.readdir() many times, when finished all of files delete (in big directory), we can chmod the big directory
  largeDirMap to record the times of invoke *File.readdir(),  largeDirMutex for avoid concurrent access 
 */
var largeDPMap = map[string]int64{}
var largeDPMutex sync.Mutex

func walkDir(srcDir string,  nDir *sync.WaitGroup, dfPairChan chan<- map[DirInfo][]string, dirSema chan struct{}) {
        defer nDir.Done()

	dirF, err := os.Open(srcDir)
	if err != nil {
                logger.Printf("\t os.Open('%s') error: %v\n", srcDir, err)
                return
	}
	var dirList = make([]string, 0)
	for {
		var fileList = make([]string, 0)
		var dirCount, fileCount, totalSize int64

		entrys, err := dirF.Readdir(readdirCount)
		if err != nil && err != io.EOF {
			logger.Printf("\t (*File).Readdir(%d) error: %v\n", readdirCount, err)
			return
		} 

		for _, entry := range entrys {
			if entry.IsDir() {
				dirCount++
				subSrcDir := filepath.Join(srcDir, entry.Name())
				dirList = append(dirList, subSrcDir)
			} else {
				fileCount++
				totalSize += entry.Size()

				s := entry.Name()
				if ! strings.HasPrefix(s, "version.") { 
					continue
				}
				if ! strings.HasSuffix(s, ".data") {
					continue
				}
				arrays := strings.Split(s, ".")
				if arrays[0] != "version" || len(arrays) < 2 {
					continue
				}
				var deleteTime, gap	int64
				if deleteTime, err = strconv.ParseInt(arrays[1], 10, 64); err != nil {
					continue
				}
				gap = time.Now().Unix() - deleteTime
				if gap <= (3600 * versionHours) {
					continue
				}	
				srcFile := filepath.Join(srcDir, entry.Name())
				fileList = append(fileList, srcFile)
			}
		}
		var di	DirInfo
		di.srcDir = srcDir
		di.totalSize = totalSize
		di.dirCount = dirCount
		di.fileCount = fileCount

		if len(entrys) == readdirCount {
			di.toBeContinue = true
		}
		dfPair := make(map[DirInfo][]string)
		dfPair[di] = fileList
		dfPairChan <- dfPair
	
		if len(entrys) < readdirCount { // readdir completed
			break
		}
	}
	// close the opened directory
	dirF.Close()
        func() { <-dirSema }()

	for _, dir2 := range dirList {
		nDir.Add(1)
		dirSema <- struct{}{}
		go walkDir(dir2, nDir, dfPairChan, dirSema)
	}
		
}

func doOneDirFileDelete(di DirInfo, fileList []string, diChan chan<- DirInfo) DirInfo {
	for _, fp := range fileList {
		fn := doFileDelete(fp)
		di.totalSize += fn.srcSize
		di.fileCount += 1
		if fn.err == false { 
			di.totalDeleteSize += fn.deleteSize
			di.deleteFileCount++
		}
	}
	diChan <- di
	return di
}

func doFileDelete(srcFile string) FileNode {
	var sfi	os.FileInfo
	var err	error 
	var fn FileNode

	if sfi, err = os.Lstat(srcFile); os.IsNotExist(err) {
		logger.Printf("\t '%s' is not exists, continue... ", srcFile)
		return fn
	}
	if verbose >= 2 {
		logger.Printf("\t delete '%s'", srcFile)
	}
	fn.srcFile = srcFile
	fn.srcSize = sfi.Size()
	if err = os.Remove(srcFile); err != nil {
		logger.Printf("\t delete '%s' error ", srcFile)
		fn.err = true
		fn.deleteSize = 0
		return fn
	}
	fn.deleteSize = sfi.Size()
	return fn
}

func main() {
	flag.IntVar(&dirWorkers, "dirWorker", DIRWORKERS, "concurrent walk directory workers")
	flag.IntVar(&fileWorkers, "fileWorker", FILEWORKERS, "concurrent file delete workers")
	flag.Int64Var(&versionHours, "versionHours", VERSIONHOURS, "version hours away from now")
	flag.StringVar(&logfile, "logfile", "/tmp/OSSVersionDelete.log", "log filename")
	flag.IntVar(&verbose, "verbose", 0, "verbose message, 0 for normal messages, 1 for +directorys messages, >=2 for +directorys and +files messages")
	flag.IntVar(&readdirCount, "readdirCount", READDIRCOUNT, "max entry every (*File).Readdir(), when read huge directory")

        flag.Parse()
        args := flag.Args()
        if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] Directory\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

        l, err := os.OpenFile(logfile, os.O_APPEND | os.O_RDWR | os.O_CREATE, 0755)
        if err != nil {
                fmt.Fprintf(os.Stderr, "os.OpenFile('%s') error: %v", logfile, err)
		os.Exit(2)
        }
        defer l.Close()
	logger = log.New(l, "", log.LstdFlags)

	srcDir := args[0]
	absSrcDir, err := filepath.Abs(srcDir)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(absSrcDir); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "ERROR: %s is not exists, Quit!\n", absSrcDir)
		os.Exit(2)
	}

	startTime := time.Now().Unix()
	logger.Printf("\t #############################  BEGIN  #########################################################\n")
	logger.Printf("\t User['%d'] Begin to DELETE version file ['%s'].....\n", euid, absSrcDir)

	dirSema := make(chan struct{}, dirWorkers)
	fileSema := make(chan struct{}, fileWorkers)
        dfPairChan := make(chan map[DirInfo][]string, fileWorkers)
	diChanLen := fileWorkers
	diChan := make(chan DirInfo, diChanLen)
        var nDir sync.WaitGroup

	nDir.Add(1)
       	dirSema <- struct{}{}
	go walkDir(absSrcDir, &nDir, dfPairChan, dirSema)

        go func() {
                nDir.Wait()
                close(dfPairChan)
        }()

	var nFile sync.WaitGroup
	go func() {
		for difp := range dfPairChan {
			for di, fileList := range difp {
				nFile.Add(1)
				fileSema <- struct{}{}

				largeDPMutex.Lock()	
				var tmp	string
				srcDir = di.srcDir
				if len(fileList) == readdirCount {
					largeDPMap[tmp]++
				}
				largeDPMutex.Unlock()

				go func() {
					defer nFile.Done()
					defer func() { <-fileSema }()
					var taskId = fmt.Sprintf("%x", md5.Sum([]byte(di.srcDir)))
					if verbose >= 1 {
						logger.Printf("\t %s: start clear directory: ['%s'] , dirWorkers:[%d/%d], fileWorkers:[%d/%d]\n", taskId, di.srcDir, len(dirSema), dirWorkers, len(fileSema), fileWorkers)
					}
					di = doOneDirFileDelete(di, fileList, diChan)
					if verbose >= 1 {
						if di.toBeContinue {
							logger.Printf("\t %s: partial finish delete '%s', to be continue ....... \n", taskId, di.srcDir)
						} else {
							logger.Printf("\t %s: finish delete '%s'\n", taskId, di.srcDir)
						}
						logger.Printf("\t %s: dirs[%s] files[%s], totalSize[%s]bytes deleteFiles[%s] totalDeleteSize[%s]bytes, err[%s]\n", 
							taskId, V(di.dirCount), V(di.fileCount), V(di.totalSize), V(di.deleteFileCount), V(di.totalDeleteSize), V(di.errCount))
					}
					/* Large directory need many-times readdir(), it should be restore source directory permission the last times invoke,
					   otherwise when source directory is ownned by root, maybe not-root user has no permission to write the directory
					 */
					largeDPMutex.Lock()	
					var tmp string
					tmp = di.srcDir
					if largeDPMap[tmp] > 0 && len(fileList) == readdirCount {
						largeDPMap[tmp]--
					}
					if largeDPMap[tmp] == 0 {
						delete(largeDPMap, tmp)
					}
					largeDPMutex.Unlock()
				}()
			}
		}
               	nFile.Wait()
               	close(diChan)
	}()

	var allDirCount, allFileCount, allTotalSize, allDeleteFileCount, allTotalDeleteSize, allErrCount	int64
	var timeElasped, speed int64
	for di := range diChan {
		allDirCount += di.dirCount
		allFileCount += di.fileCount
		allDeleteFileCount += di.deleteFileCount
		allTotalSize += di.totalSize
		allTotalDeleteSize += di.totalDeleteSize
		allErrCount += di.errCount
		timeElasped = time.Now().Unix() - startTime
		if timeElasped > 0 {
			speed = allFileCount / timeElasped
		}
		if di.deleteFileCount > 0 {
        		logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        		logger.Printf("\t current progress: Directorys: [%s], Files: [%s], allTotalSrcSize: [%s] bytes, diChan:[%d/%d], speed[%d/s], Elasped[%s seconds]\n", 
					V(allDirCount), V(allFileCount), V(allTotalSize), len(diChan), diChanLen, speed, V(timeElasped))
        		logger.Printf("\t                   allDeleteFileCount: [%s], allTotalDeleteSize: [%s] bytes, allErr: [%s]\n", V(allDeleteFileCount), V(allTotalDeleteSize), V(allErrCount))
        		logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
		}
	}
	timeElasped = time.Now().Unix() - startTime
	if timeElasped > 0 {
		speed = allFileCount / timeElasped
	}
        logger.Printf("\t Finished CLEAR version file ['%s']\n", absSrcDir)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t Summary: Directorys: [%s], Files: [%s], allTotalSrcSize: [%s] bytes, speed[%d/s], Elasped: [%s seconds]\n", V(allDirCount), V(allFileCount), V(allTotalSize), speed, V(timeElasped))
        logger.Printf("\t          allDeleteFileCount: [%s], allTotalDeleteSize: [%s] bytes, allErr: [%s]\n", V(allDeleteFileCount), V(allTotalDeleteSize), V(allErrCount))
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t ############################### END #############################################################\n\n\n")

}


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

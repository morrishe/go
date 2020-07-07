package main

import (
        "os"
        "fmt"
        "flag"
        "log"
        "io"
        "path/filepath"
        "sync"
        "time"
)

type DirPairInfo struct {
        srcDir          string
        dstDir          string
	fileCount	int64
	dirCount	int64
	totalCopySize	int64
	totalSkipSize	int64
	skipFileCount	int64
}

type DirPair struct {
	srcDir		string
	dstDir		string
}

type FilePair struct {
        srcFile         string
        dstFile         string
}

const (
	DIRWORKERS = 32
	READDIRCOUNT = 4096
)

var dirWorkers	int
var readdirCount	int
var logfile     string
var logger      *log.Logger

func walkDir(dstDir string, srcDir string,  nDir *sync.WaitGroup, dfPairChan chan<- map[DirPairInfo][]FilePair, dirSema chan struct{}) {
        defer nDir.Done()

	dirF, err := os.Open(srcDir)
	if err != nil {
                logger.Printf("\t os.Open('%s') error: %v\n", dstDir, err)
                return
	}
	var dpList = make([]DirPair, 0)
	for {
		var fpList = make([]FilePair, 0)
		var dirCount, fileCount, totalCopySize, skipFileCount, totalSkipSize int64

		entrys, err := dirF.Readdir(readdirCount)
		if err != nil && err != io.EOF {
			logger.Printf("\t (*File).Readdir(%d) error: %v\n", readdirCount, err)
			return
		} 

		for _, entry := range entrys {
			if entry.IsDir() {
				dirCount++
				subSrcDir := filepath.Join(srcDir, entry.Name())
				subDstDir := filepath.Join(dstDir, entry.Name())

				var dp DirPair
				dp.srcDir = subSrcDir
				dp.dstDir = subDstDir
				dpList = append(dpList, dp)
			} else {
				var fp FilePair
				var sfi, dfi os.FileInfo
				fp.srcFile = filepath.Join(srcDir, entry.Name())
				fp.dstFile = filepath.Join(dstDir, entry.Name())
  				sfi, err = os.Lstat(fp.srcFile)
  				dfi, err = os.Lstat(fp.dstFile)
                		if os.IsNotExist(err) { 
					totalCopySize += entry.Size()
                		} else if dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
					totalCopySize += entry.Size()
				} else {
					skipFileCount++
					totalSkipSize += entry.Size()	
					continue
				}
				fpList = append(fpList, fp)
				fileCount++
			}
		}
		var dpi	DirPairInfo
		dpi.srcDir = srcDir
		dpi.dstDir = dstDir
		dpi.totalCopySize = totalCopySize
		dpi.dirCount = dirCount
		dpi.fileCount = fileCount
		dpi.skipFileCount = skipFileCount
		dpi.totalSkipSize = totalSkipSize

		dfPair := make(map[DirPairInfo][]FilePair)
		dfPair[dpi] = fpList
		dfPairChan <- dfPair
	
		if len(entrys) < readdirCount { // readdir completed
			break
		}
	}
	// close the opened directory
	dirF.Close()
        func() { <-dirSema }()

	for _, dp2 := range dpList {
		nDir.Add(1)
		dirSema <- struct{}{}
		go walkDir(dp2.dstDir, dp2.srcDir, nDir, dfPairChan, dirSema)
	}
		
}


func main() {
	flag.IntVar(&dirWorkers, "dirWorker", DIRWORKERS, "concurrent walk directory workers")
	flag.IntVar(&readdirCount, "readdirCount", READDIRCOUNT, "max entry every (*File).Readdir(), when read huge directory")
	flag.StringVar(&logfile, "logfile", "/tmp/BackupCal.log", "log filename")

        flag.Parse()
        args := flag.Args()
        if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] srcDir dstDir\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

        l, err := os.OpenFile(logfile, os.O_TRUNC | os.O_RDWR | os.O_CREATE, 0755)
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
	dstDir := args[1]
	absDstDir, err := filepath.Abs(dstDir)
	if err != nil {
		log.Fatal(err)
	}
	startTime := time.Now().Unix()
	logger.Printf("\t #############################  BEGIN  #########################################################\n")
	logger.Printf("\t User['%d'] Begin to calculate ['%s'] to ['%s'].....\n", os.Geteuid(), absSrcDir, absDstDir)

	dirSema := make(chan struct{}, dirWorkers)
        dfPairChan := make(chan map[DirPairInfo][]FilePair, dirWorkers)
        var nDir sync.WaitGroup

	nDir.Add(1)
       	dirSema <- struct{}{}
	go walkDir(absDstDir, absSrcDir, &nDir, dfPairChan, dirSema)

        go func() {
                nDir.Wait()
                close(dfPairChan)
        }()

	var dpi DirPairInfo
	var allFpList, fpList []FilePair
	var allDirCount, allFileCount, allTotalCopySize, allTotalSkipSize, allSkipFileCount, timeElasped int64
	for dpifp := range dfPairChan {
		for dpi, fpList = range dpifp {
			allDirCount += dpi.dirCount
			allFileCount += dpi.fileCount
			allTotalCopySize += dpi.totalCopySize
			allTotalSkipSize += dpi.totalSkipSize
			allSkipFileCount += dpi.skipFileCount
			allFpList = append(allFpList, fpList...)
			timeElasped = time.Now().Unix() - startTime
        		fmt.Printf("\t Current progress: directorys: [%s], allCopyFiles: [%s], allTotalCopySize: [%s], allSkipFiles: [%s], allTotalSkipSize: [%s] bytes, Elasped: [%d seconds]\n", V(allDirCount), 
				V(allFileCount), V(allTotalCopySize), V(allSkipFileCount), V(allTotalSkipSize), timeElasped)
		}
	}

	timeElasped = time.Now().Unix() - startTime
        logger.Printf("\t Finished statistics ['%s'] to ['%s']\n", absSrcDir, absDstDir)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t Summary: Directorys: [%d], Files: [%d], allTotalCopySize: [%d] bytes, allSkipFiles: [%d], allTotalSkipSize: [%d] bytes, Elasped: [%d seconds]\n", allDirCount, allFileCount, allTotalCopySize, 
			allSkipFileCount, allTotalSkipSize, timeElasped)
	logger.Printf("\t NEED COPY FILES as below:\n")
	for _, fp := range allFpList {
		logger.Printf("\t %s, %s\n", fp.srcFile, fp.dstFile)
	} 
	logger.Printf("\t End of NEED COPY FILES\n")
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


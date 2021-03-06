package main

/*
#include <fcntl.h>
#include <sys/stat.h>

int set_symlink_timestamp(const char *pathname, struct timespec atime, struct timespec mtime)
{
	int	ret;
	struct timespec	ts[2];
	ts[0] = atime;
	ts[1] = mtime;
	ret = utimensat(AT_FDCWD, pathname, ts, AT_SYMLINK_NOFOLLOW);
	return ret;
}
*/
import "C"

import (
	"os"
	"syscall"
	"fmt"
	//"strings"
	"flag"
	"log"
	"io"
	//"io/ioutil"
	"path/filepath"
	"sync"
	"time"
	"crypto/md5"
)

type DirPair struct {
        srcDir          string
        dstDir          string
	fileCount	int64
	copyFileCount	int64
	dirCount	int64
	totalSize	int64
	totalCopySize	int64
	unsupportCount	int64
	skipCount	int64
	errCount	int64
	toBeContinue	bool
}

type FilePair struct {
        srcFile         string
        dstFile         string
}

type FileNode struct {
        srcFile         string
        dstFile         string
        unsupport       bool
        skip            bool
        err             bool
        srcSize         int64
        copySize        int64
}

const (
	DIRWORKERS = 1024
	FILEWORKERS = 4096
	READDIRCOUNT = 4096
)

var dirWorkers	int
var fileWorkers	int
// log file, default '/tmp/NASCopy.log'
var logfile	string
var logger	*log.Logger
var verbose	int
var readdirCount	int
var keepNewer	bool


/* when a big directory is encounted, we need invoke *File.readdir() many times, when finished all of files copy (in big directory), we can chmod the big directory
  bigDirMap to record the times of invoke *File.readdir(),  bdmMutex for avoid concurrent access 
 */
var bigDirMap =	map[DirPair]int64{}
var bdmMutex sync.Mutex

/*  old readdir method
func dirents(dir string) []os.FileInfo {
        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                logger.Printf("\t ioutil.ReadDir('%s') error: %v\n", dir, err)
                return nil
        }
        return entries
}
*/

func walkDir(dstDir string, srcDir string,  nDir *sync.WaitGroup, dfPairChan chan<- map[DirPair][]FilePair, dirSema chan struct{}) {
        defer nDir.Done()
        defer func() { <-dirSema }()

	fp, err := os.Open(srcDir)
	if err != nil {
                logger.Printf("\t os.Open('%s') error: %v\n", dstDir, err)
                return
	}
	defer fp.Close()

	for {
		var fpList = make([]FilePair, 0)
		var dirCount, fileCount, totalSize int64

		entrys, err := fp.Readdir(readdirCount)
		if err != nil && err != io.EOF {
			logger.Printf("\t (*File).Readdir(%d) error: %v\n", readdirCount, err)
			return
		} 
			
		for _, entry := range entrys {
			if entry.Name() == ".snapshot" && entry.IsDir() {  /* skip NAS .snapshot directory */
				continue
			}
			if entry.IsDir() {
				dirCount++
				subSrcDir := filepath.Join(srcDir, entry.Name())
				subDstDir := filepath.Join(dstDir, entry.Name())
				// when walk directory tree, mkdir dstDir
				if _, e := os.Lstat(subDstDir);  os.IsNotExist(e) {
					os.MkdirAll(subDstDir, 0755)
				}
				nDir.Add(1)
        			dirSema <- struct{}{}
				go walkDir(subDstDir, subSrcDir, nDir, dfPairChan, dirSema)
			} else {
				fileCount++
				totalSize += entry.Size()
				var fp FilePair
				fp.srcFile = filepath.Join(srcDir, entry.Name())
				fp.dstFile = filepath.Join(dstDir, entry.Name())
				fpList = append(fpList, fp)
			}
		}
		var dp	DirPair
		dp.srcDir = srcDir
		dp.dstDir = dstDir
		dp.totalSize = totalSize
		dp.dirCount = dirCount
		dp.fileCount = fileCount
		if len(entrys) == readdirCount {
			dp.toBeContinue = true
		}

		bdmMutex.Lock()	
		bigDirMap[dp]++
		bdmMutex.Unlock()

		dfPair := make(map[DirPair][]FilePair)
		dfPair[dp] = fpList
		dfPairChan <- dfPair
	
		if len(entrys) < readdirCount { // readdir completed
			break	
		}
	}
}

func doOneDirFileCopy(dp DirPair, fpList []FilePair, dpChan chan<- DirPair) DirPair {
	for _, fp := range fpList {
		fn := doFileCopy(fp.dstFile, fp.srcFile)
		if !fn.skip {
			copyFileDirAttr(fp.dstFile, fp.srcFile)
		}
		switch {
		case fn.unsupport: dp.unsupportCount++
		case fn.skip:	dp.skipCount++
		case fn.err:	dp.errCount++
		default:
			dp.totalCopySize += fn.copySize
			dp.copyFileCount++
		}
	}
	dpChan <- dp
	return dp
}

func doFileCopy(dstFile, srcFile string) FileNode {
	var sfi, dfi	os.FileInfo
	var err	error 
	var isSymlink, isRegular, isUnsupport	 bool
	var fn FileNode
	var needCopy bool

	if sfi, err = os.Lstat(srcFile); os.IsNotExist(err) {
		logger.Printf("\t '%s' is not exists, continue... ", srcFile)
		return fn
	}
	if verbose >= 2 {
		logger.Printf("\t copy '%s' to '%s' ", srcFile, dstFile)
	}
	fn.srcFile = srcFile
	fn.dstFile = dstFile
	fn.srcSize = sfi.Size()

	mode := sfi.Mode()
	switch {
	case mode&os.ModeSymlink != 0:
		isSymlink = true
	case mode.IsRegular():
		isRegular = true
	default:
		isUnsupport = true
	}

	if isUnsupport {
		fn.unsupport = true
		return fn
	}

	if isSymlink { //symblink file
		os.Remove(dstFile)
		if link, err := os.Readlink(srcFile); err != nil {
			logger.Printf("\t os.Readlink('%s') error: %v\n", srcFile, err)
			fn.err = true
			return fn
		} else {
			err = os.Symlink(link, dstFile)
			if err != nil {
				logger.Printf("\t os.Symlink('%s', '%s') error: %v", link, dstFile, err)
				fn.err = true
				return fn
			}
			fn.copySize = int64(len(link))
			return fn
		}
	}
	if isRegular {
		dfi, err = os.Lstat(dstFile)
		if os.IsNotExist(err) { 
			needCopy = true
		} else if dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
			needCopy = true
			if keepNewer == true && dfi.ModTime().Unix() > sfi.ModTime().Unix() { 
				needCopy = false
			}
		} else { // file exist and the same size and the mtime is same, it should be same file
			needCopy = false
		}
		if needCopy == true {
			wtSize, err := doRegularFileCopy(dstFile, srcFile)
			if (err != nil) {
				fn.err = true
				return fn
			}
			fn.copySize = wtSize
			return fn
		} else {
			fn.skip = true
			return fn
		}
	}
	return fn
}


func doRegularFileCopy(dstFile string, srcFile string) (int64, error) {
	var sf, df	*os.File
	var err		error 
	var writtenSize int64

	if sf, err = os.Open(srcFile); err != nil {
		logger.Printf("\t os.Open('%s') error: %v\n", srcFile, err)
		return 0, err 
	}
	defer sf.Close()
	if df, err = os.Create(dstFile); err != nil {
		logger.Printf("\t os.Create('%s') error: %v\n", dstFile, err)
		return 0, err
	}
	defer df.Close()
	writtenSize, err = io.Copy(df, sf)
	return writtenSize, err
}

func copyFileDirAttr(dst string, src string) error {
	//fmt.Println(dst, src)
	if fi, err := os.Lstat(src); err == nil {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			uid := int(st.Uid)
			gid := int(st.Gid)
			atim := st.Atim
			mtim := st.Mtim

			atime := time.Unix(atim.Sec, atim.Nsec)
			mtime := time.Unix(mtim.Sec, mtim.Nsec)
			mode := fi.Mode()
			if mode&os.ModeSymlink == 0 { //not symlink
				if e := os.Chmod(dst, mode); e != nil {
					//logger.Printf("\t chmod(%s, %v) error\n", dst, mode)
				}
				if e := os.Chtimes(dst, atime, mtime); e != nil {
					//logger.Printf("\t os.Chtimes(%s, %v, %v) error\n", dst, atime, mtime)
				}
			} else { //symlink
				at := C.struct_timespec{C.long(atim.Sec), C.long(atim.Nsec)}
				mt := C.struct_timespec{C.long(mtim.Sec), C.long(mtim.Nsec)}
				C.set_symlink_timestamp(C.CString(dst), at, mt)
			}
			euid := os.Geteuid()
			if euid == 0 {
				if e := os.Lchown(dst, uid, gid); e != nil {
					//logger.Printf("\t chown(%s, %d, %d) error\n", dst, uid, gid)
				}
			}
		}
	}
	return nil
}

func main() {
	flag.IntVar(&dirWorkers, "dirWorker", DIRWORKERS, "concurrent walk directory workers")
	flag.IntVar(&fileWorkers, "fileWorker", FILEWORKERS, "concurrent file copy workers")
	flag.StringVar(&logfile, "logfile", "/tmp/NASCopy.log", "log filename")
	flag.IntVar(&verbose, "verbose", 0, "verbose message, 0 null message, 1 for dir, >=2 for dir and files")
	flag.IntVar(&readdirCount, "readdirCount", READDIRCOUNT, "max entry every (*File).Readdir(), when read huge directory")
	flag.BoolVar(&keepNewer, "keepNewer", false, "default override destination file, enable this option will keep *NEWER* destination file")

        flag.Parse()
        args := flag.Args()
        if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] srcDir dstDir\n", os.Args[0])
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
	dstDir := args[1]
	absDstDir, err := filepath.Abs(dstDir)
	if err != nil {
		log.Fatal(err)
	}
	if _, err = os.Lstat(absDstDir);  os.IsNotExist(err) {
		os.MkdirAll(absDstDir, 0755)
	}

	startTime := time.Now().Unix()
	logger.Printf("\t #############################  BEGIN  #########################################################\n")
	logger.Printf("\t Begin to COPY ['%s'] to ['%s'].....\n", absSrcDir, absDstDir)

	dirSema := make(chan struct{}, dirWorkers)
	fileSema := make(chan struct{}, fileWorkers)
        dfPairChan := make(chan map[DirPair][]FilePair, fileWorkers)
	dpChanLen := fileWorkers
	dpChan := make(chan DirPair, dpChanLen)
        var nDir sync.WaitGroup

	nDir.Add(1)
       	dirSema <- struct{}{}
	go walkDir(absDstDir, absSrcDir, &nDir, dfPairChan, dirSema)

        go func() {
                nDir.Wait()
                close(dfPairChan)
        }()

	var nFile sync.WaitGroup
	go func() {
		for dpfp := range dfPairChan {
			for dp, fpList := range dpfp {
				nFile.Add(1)
				fileSema <- struct{}{}
				go func() {
					defer nFile.Done()
					defer func() { <-fileSema }()
					var taskId = fmt.Sprintf("%x", md5.Sum([]byte(dp.srcDir)))
					if verbose >= 1 {
						logger.Printf("\t %s: start copy ['%s'] to ['%s'], dirWorkers:[%d/%d], fileWorkers:[%d/%d]\n", taskId, dp.srcDir, dp.dstDir, len(dirSema), dirWorkers, len(fileSema), fileWorkers)
					}
					dp = doOneDirFileCopy(dp, fpList, dpChan)
					if verbose >= 1 {
						if dp.toBeContinue {
							logger.Printf("\t %s: partial finish copy '%s' to '%s', to be continue ....... \n", taskId, dp.srcDir, dp.dstDir)
						} else {
							logger.Printf("\t %s: finish copy '%s' to '%s'\n", taskId, dp.srcDir, dp.dstDir)
						}
						logger.Printf("\t %s: dirs[%d] files[%d], totalSize[%d]bytes copyFiles[%d] totalCopySize[%d]bytes unsupport[%d] skip[%d] err[%d]\n", 
							taskId, dp.dirCount, dp.fileCount, dp.totalSize, dp.copyFileCount, dp.totalCopySize, dp.unsupportCount, dp.skipCount, dp.errCount)
					}
					bdmMutex.Lock()	
					bigDirMap[dp]--
					if bigDirMap[dp] == 0 {
						copyFileDirAttr(dp.dstDir, dp.srcDir)
						delete(bigDirMap, dp)
					} else {
						fmt.Printf(" bigDirMap[%s]: %d\n", dp.dstDir, bigDirMap[dp])
					}
					bdmMutex.Unlock()
				}()
			}
		}
               	nFile.Wait()
               	close(dpChan)
	}()

	var allDirCount, allFileCount, allTotalSize, allCopyFileCount, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount	int64
	for dp := range dpChan {
		allDirCount += dp.dirCount
		allFileCount += dp.fileCount
		allCopyFileCount += dp.copyFileCount
		allTotalSize += dp.totalSize
		allTotalCopySize += dp.totalCopySize
		allUnsupportCount += dp.unsupportCount
		allSkipCount += dp.skipCount
		allErrCount += dp.errCount
		timeElasped := time.Now().Unix() - startTime
		var speed int64
		if timeElasped > 0 {
			speed = allFileCount / timeElasped
		}
		if dp.copyFileCount > 0 {
        		logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        		logger.Printf("\t current progress: Files: [%d], allTotalSrcSize: [%d] bytes, dpChan:[%d/%d], speeds[%d/s], Elasped[%d sec]\n", allFileCount, allTotalSize, len(dpChan), dpChanLen, speed, timeElasped)
        		logger.Printf("\t                   allCopyFileCount: %d, allTotalCopySize: %d bytes, allUnsupport: %d, allSkip: %d, allErr: %d\n", allCopyFileCount, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
        		logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
		}
	}
        logger.Printf("\t Finished COPY ['%s'] to ['%s']\n", absSrcDir, absDstDir)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t Summary: Directorys: %d, Files: %d, allTotalSrcSize: %d bytes\n", allDirCount, allFileCount, allTotalSize)
        logger.Printf("\t          allCopyFileCount: %d, allTotalCopySize: %d bytes, allUnsupport: %d, allSkip: %d, allErr: %d\n",
			allCopyFileCount, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t ############################### END #############################################################\n\n\n")

}

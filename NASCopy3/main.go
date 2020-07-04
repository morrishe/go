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

type DirPairInfo struct {
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

type DirPair struct {
	srcDir		string
	dstDir		string
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
var euid = os.Geteuid()
var ABSSRCDIR	string

// not include the directorys and files in the 'NASCopy.exclude' which is default , per dir|file per line
var excludeFrom	string
var excludeDirMap = map[string]bool{}
var excludeFileMap = map[string]bool{}

func parseExcludeFrom(exFrom string, dMap, fMap map[string]bool) error {
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

/* when a big directory is encounted, we need invoke *File.readdir() many times, when finished all of files copy (in big directory), we can chmod the big directory
  largeDirMap to record the times of invoke *File.readdir(),  largeDirMutex for avoid concurrent access 
 */
var largeDPMap = map[DirPair]int64{}
var largeDPMutex sync.Mutex

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
		var dirCount, fileCount, totalSize int64

		entrys, err := dirF.Readdir(readdirCount)
		if err != nil && err != io.EOF {
			logger.Printf("\t (*File).Readdir(%d) error: %v\n", readdirCount, err)
			return
		} 

		for _, entry := range entrys {
			if entry.Name() == ".snapshot" && entry.IsDir() {  /* skip NAS .snapshot directory */
				continue
			}
			if entry.IsDir() {
				{
					fullPathName := filepath.Join(srcDir, entry.Name())
					keyName := fullPathName[len(ABSSRCDIR)+1:]
					if excludeDirMap[keyName] || excludeDirMap[entry.Name()] {
						logger.Printf("\t                   '%s' is '%s' in exclude directory list, ignore\n", fullPathName, keyName)
						continue
					}
				}	
				dirCount++
				subSrcDir := filepath.Join(srcDir, entry.Name())
				subDstDir := filepath.Join(dstDir, entry.Name())
				// when walk directory tree, mkdir dstDir
				if _, e := os.Lstat(subDstDir);  os.IsNotExist(e) {
					os.MkdirAll(subDstDir, 0755)
				}
				var dp DirPair
				dp.srcDir = subSrcDir
				dp.dstDir = subDstDir
				dpList = append(dpList, dp)
			} else {
				{
					fullPathName := filepath.Join(srcDir, entry.Name())
					keyName := fullPathName[len(ABSSRCDIR)+1:]
					if excludeFileMap[keyName] || excludeFileMap[entry.Name()] {
						logger.Printf("\t                   '%s' is '%s' in exclude file list, ignore\n", fullPathName, keyName)
						continue
					}
				}	
				fileCount++
				totalSize += entry.Size()
				var fp FilePair
				fp.srcFile = filepath.Join(srcDir, entry.Name())
				fp.dstFile = filepath.Join(dstDir, entry.Name())
				fpList = append(fpList, fp)
			}
		}
		var dpi	DirPairInfo
		dpi.srcDir = srcDir
		dpi.dstDir = dstDir
		dpi.totalSize = totalSize
		dpi.dirCount = dirCount
		dpi.fileCount = fileCount

		if len(entrys) == readdirCount {
			dpi.toBeContinue = true
		}
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

func doOneDirFileCopy(dpi DirPairInfo, fpList []FilePair, dpiChan chan<- DirPairInfo) DirPairInfo {
	for _, fp := range fpList {
		fn := doFileCopy(fp.dstFile, fp.srcFile)
		if !fn.skip {
			copyFileDirAttr(fp.dstFile, fp.srcFile)
		}
		switch {
		case fn.unsupport: dpi.unsupportCount++
		case fn.skip:	dpi.skipCount++
		case fn.err:	dpi.errCount++
		default:
			dpi.totalCopySize += fn.copySize
			dpi.copyFileCount++
		}
	}
	dpiChan <- dpi
	return dpi
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
	flag.StringVar(&excludeFrom, "excludeFrom", "NASCopy.exclude", "directory and file in this file is not been copied")

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
	ABSSRCDIR = absSrcDir
	dstDir := args[1]
	absDstDir, err := filepath.Abs(dstDir)
	if err != nil {
		log.Fatal(err)
	}
	if _, err = os.Lstat(absDstDir);  os.IsNotExist(err) {
		os.MkdirAll(absDstDir, 0755)
	}
	
	if err = parseExcludeFrom(excludeFrom, excludeDirMap, excludeFileMap); err != nil {
		//do nothing
	}
	var excludeDirList, excludeFileList	[]string
	for d, _ := range excludeDirMap {
		excludeDirList = append(excludeDirList, d)
	} 
	for f, _ := range excludeFileMap {
		excludeFileList = append(excludeFileList, f)
	} 

	startTime := time.Now().Unix()
	logger.Printf("\t #############################  BEGIN  #########################################################\n")
	logger.Printf("\t User['%d'] Begin to COPY ['%s'] to ['%s'].....\n", euid, absSrcDir, absDstDir)
	logger.Printf("\t File: ['%s'] has exclude list:\n", excludeFrom)
	logger.Printf("\t Exclude Dir:  %v\n", excludeDirList)
	logger.Printf("\t Exclude File: %v\n", excludeFileList)

	dirSema := make(chan struct{}, dirWorkers)
	fileSema := make(chan struct{}, fileWorkers)
        dfPairChan := make(chan map[DirPairInfo][]FilePair, fileWorkers)
	dpiChanLen := fileWorkers
	dpiChan := make(chan DirPairInfo, dpiChanLen)
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
		for dpifp := range dfPairChan {
			for dpi, fpList := range dpifp {
				nFile.Add(1)
				fileSema <- struct{}{}

				if euid != 0 {
					largeDPMutex.Lock()	
					var tmp	DirPair
					tmp.srcDir = dpi.srcDir
					tmp.dstDir = dpi.dstDir
					largeDPMap[tmp]++
					largeDPMutex.Unlock()
				}

				go func() {
					defer nFile.Done()
					defer func() { <-fileSema }()
					var taskId = fmt.Sprintf("%x", md5.Sum([]byte(dpi.srcDir)))
					if verbose >= 1 {
						logger.Printf("\t %s: start copy ['%s'] to ['%s'], dirWorkers:[%d/%d], fileWorkers:[%d/%d]\n", taskId, dpi.srcDir, dpi.dstDir, len(dirSema), dirWorkers, len(fileSema), fileWorkers)
					}
					dpi = doOneDirFileCopy(dpi, fpList, dpiChan)
					if verbose >= 1 {
						if dpi.toBeContinue {
							logger.Printf("\t %s: partial finish copy '%s' to '%s', to be continue ....... \n", taskId, dpi.srcDir, dpi.dstDir)
						} else {
							logger.Printf("\t %s: finish copy '%s' to '%s'\n", taskId, dpi.srcDir, dpi.dstDir)
						}
						logger.Printf("\t %s: dirs[%d] files[%d], totalSize[%d]bytes copyFiles[%d] totalCopySize[%d]bytes unsupport[%d] skip[%d] err[%d]\n", 
							taskId, dpi.dirCount, dpi.fileCount, dpi.totalSize, dpi.copyFileCount, dpi.totalCopySize, dpi.unsupportCount, dpi.skipCount, dpi.errCount)
					}
					/* Large directory need many-times readdir(), it should be restore source directory permission the last times invoke,
					   otherwise when source directory is ownned by root, maybe not-root user has no permission to write the directory
					 */
					if euid != 0 {
						largeDPMutex.Lock()	
						var tmp DirPair
						tmp.srcDir = dpi.srcDir
						tmp.dstDir = dpi.dstDir
						largeDPMap[tmp]--
						if largeDPMap[tmp] == 0 {
							copyFileDirAttr(tmp.dstDir, tmp.srcDir)
							delete(largeDPMap, tmp)
						}
						largeDPMutex.Unlock()
					} else {
						copyFileDirAttr(dpi.dstDir, dpi.srcDir)
					}
				}()
			}
		}
               	nFile.Wait()
               	close(dpiChan)
	}()

	var allDirCount, allFileCount, allTotalSize, allCopyFileCount, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount	int64
	var timeElasped, speed int64
	for dpi := range dpiChan {
		allDirCount += dpi.dirCount
		allFileCount += dpi.fileCount
		allCopyFileCount += dpi.copyFileCount
		allTotalSize += dpi.totalSize
		allTotalCopySize += dpi.totalCopySize
		allUnsupportCount += dpi.unsupportCount
		allSkipCount += dpi.skipCount
		allErrCount += dpi.errCount
		timeElasped = time.Now().Unix() - startTime
		if timeElasped > 0 {
			speed = allFileCount / timeElasped
		}
		if dpi.copyFileCount > 0 {
        		logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        		logger.Printf("\t current progress: Directorys: [%d], Files: [%d], allTotalSrcSize: [%d] bytes, dpiChan:[%d/%d], speed[%d/s], Elasped[%d seconds]\n", allDirCount, allFileCount, allTotalSize, len(dpiChan), dpiChanLen, speed, timeElasped)
        		logger.Printf("\t                   allCopyFileCount: %d, allTotalCopySize: %d bytes, allUnsupport: %d, allSkip: %d, allErr: %d\n", allCopyFileCount, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
        		logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
		}
	}
	timeElasped = time.Now().Unix() - startTime
	if timeElasped > 0 {
		speed = allFileCount / timeElasped
	}
        logger.Printf("\t Finished COPY ['%s'] to ['%s'], speed[%d/s], Elasped:[%d seconds]\n", absSrcDir, absDstDir, speed, timeElasped)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t Summary: Directorys: %d, Files: %d, allTotalSrcSize: %d bytes <=> %d MB\n", allDirCount, allFileCount, allTotalSize, allTotalSize/(1024*1024))
        logger.Printf("\t          allCopyFileCount: %d, allTotalCopySize: %d bytes <=> %d MB, allUnsupport: %d, allSkip: %d, allErr: %d\n",
			allCopyFileCount, allTotalCopySize, allTotalCopySize/(1024*1024), allUnsupportCount, allSkipCount, allErrCount)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t ############################### END #############################################################\n\n\n")

}

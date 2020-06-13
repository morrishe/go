package main

import (
	"os"
	"syscall"
	"fmt"
	//"strings"
	"flag"
	"log"
	"io"
	"io/ioutil"
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
}

type FilePair struct {
        srcFile         string
        dstFile          string
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
	DIRWORKERS = 64 
	FILEWORKERS = 256
)

var dirWorkers	int
var fileWorkers	int
// log file, default '/tmp/NASCopy.log'
var logfile	string
var logger	*log.Logger
var verbose	int

func dirents(dir string) []os.FileInfo {
        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                logger.Printf("\t %v\n", err)
                return nil
        }
        return entries
}

func walkDir(dstDir string, srcDir string,  nDir *sync.WaitGroup, dfPairChan chan<- map[DirPair][]FilePair, dirSema chan struct{}) {
        defer nDir.Done()
        dirSema <- struct{}{}
        defer func() { <-dirSema }()

	entrys := dirents(srcDir)
	var fpList = make([]FilePair, 0)
	var dirCount, fileCount, totalSize int64
        for _, entry := range entrys {
		if entry.Name() == ".snapshot" && entry.IsDir() {  /* skip NAS .snapshot directory */
                        continue
                }
                if entry.IsDir() {
			dirCount++
                        subSrcDir := filepath.Join(srcDir, entry.Name())
			subDstDir := filepath.Join(dstDir, entry.Name())
                        nDir.Add(1)
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

	dfPair := make(map[DirPair][]FilePair)
	dfPair[dp] = fpList

	dfPairChan <- dfPair
}

func doOneDirFileCopy(dp DirPair, fpList []FilePair, dpChan chan<- DirPair) DirPair {
	for _, fp := range fpList {
		fn := doFileCopy(fp.dstFile, fp.srcFile)
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

	if sfi, err = os.Lstat(srcFile); os.IsNotExist(err) {
		logger.Printf("\t '%s' is not exists, continue... ")
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
		os.Remove(dstFile) // ignore error
		if link, err := os.Readlink(srcFile); err != nil {
			logger.Printf("\t os.Readlink('%s') error n", srcFile)
			fn.err = true
			return fn
		} else {
			err = os.Symlink(link, dstFile)
			if err != nil {
				logger.Printf("\t os.Symlink('%s') error n", srcFile)
				fn.err = true
				return fn
			}
			copyFileAttribute(dstFile, srcFile)
			fn.copySize = int64(len(link))
			return fn
		}
	}
	if isRegular {
		dfi, err = os.Lstat(dstFile)
		if os.IsNotExist(err) || dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
			// ModTime or Size is not same, file modified, copy it
			wtSize, err := doRegularFileCopy(dstFile, srcFile)
			if (err != nil) {
				fn.err = true
				return fn
			}
			copyFileAttribute(dstFile, srcFile)
			fn.copySize = wtSize
			return fn
		} else {
			//logger.Printf("\t %s exist and ModTime() and Size() is same, the same file, skip it\n", dstFile)
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
		fmt.Fprintf(os.Stderr, "os.Open('%s') error: %v\n", srcFile, err)
		return 0, err 
	}
	defer sf.Close()
	if df, err = os.Create(dstFile); err != nil {
		fmt.Fprintf(os.Stderr, "os.Create('%s') error: %v\n", dstFile, err)
		return 0, err
	}
	defer df.Close()
	writtenSize, err = io.Copy(df, sf)
	return writtenSize, err
}

func copyFileAttribute(dst string, src string) error {
	if fi, err := os.Lstat(src); err == nil {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			uid := int(st.Uid)
			gid := int(st.Gid)
			atim := st.Atim
			mtim := st.Mtim

			atime := time.Unix(atim.Sec, atim.Nsec)
			mtime := time.Unix(mtim.Sec, mtim.Nsec)
			mode := fi.Mode()
			if mode&os.ModeSymlink == 0 { //ignore symlink
				if e := os.Chmod(dst, mode); e != nil {
					//logger.Printf("\t chmod(%s, %v) error\n", dst, mode)
				}
				if e := os.Chtimes(dst, atime, mtime); e != nil {
					//logger.Printf("\t os.Chtimes(%s, %v, %v) error\n", dst, atime, mtime)
				}
			}
			if e := os.Lchown(dst, uid, gid); e != nil {
				//logger.Printf("\t chown(%s, %d, %d) error\n", dst, uid, gid)
			}
		}
	}
	return nil
}

func main() {
	flag.IntVar(&dirWorkers, "dirworker", DIRWORKERS, "concurrent walk directory workers")
	flag.IntVar(&fileWorkers, "fileworker", FILEWORKERS, "concurrent file copy workers")
	flag.StringVar(&logfile, "logfile", "/tmp/NASCopy.log", "log filename")
	flag.IntVar(&verbose, "verbose", 0, "verbose message")

        flag.Parse()
        args := flag.Args()
        if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] srcDir dstDir\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

        l, err := os.OpenFile(logfile, os.O_APPEND | os.O_RDWR | os.O_CREATE, 0755)
        if err != nil {
                log.Fatal(err)
        }
        defer l.Close()
	logger = log.New(l, "", log.LstdFlags)

	srcDir := args[0]
	absSrcDir, err := filepath.Abs(srcDir)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(absSrcDir); os.IsNotExist(err) {
		fmt.Printf("ERROR: %s is not exists, Quit!\n", absSrcDir)
		os.Exit(2)
	}
	dstDir := args[1]
	absDstDir, err := filepath.Abs(dstDir)
	if err != nil {
		log.Fatal(err)
	}

	logger.Printf("\t #############################  BEGIN  #########################################################\n")
	logger.Printf("\t Begin to COPY ['%s'] to ['%s'].....\n", absSrcDir, absDstDir)

	dirSema := make(chan struct{}, dirWorkers)
	fileSema := make(chan struct{}, fileWorkers)
        dpFileChan := make(chan map[DirPair][]FilePair, fileWorkers)
	dpChan := make(chan DirPair, fileWorkers)
        var nDir sync.WaitGroup

	nDir.Add(1)
	go walkDir(absDstDir, absSrcDir, &nDir, dpFileChan, dirSema)

        go func() {
                nDir.Wait()
                close(dpFileChan)
        }()

	var nFile sync.WaitGroup
	for dpfp := range dpFileChan {
		for dp, fpList := range dpfp {
			nFile.Add(1)
			var taskId = fmt.Sprintf("%x", md5.Sum([]byte(dp.srcDir)))
			logger.Printf("\t %s: start copy ['%s'] to ['%s'], dirWorkers:[%d], fileWorkers:[%d]\n", taskId, dp.srcDir, dp.dstDir, len(dirSema), len(fileSema))
			go func() {
				defer nFile.Done()
				fileSema <- struct{}{}
				defer func() { <-fileSema }()
				if _, err = os.Lstat(dp.dstDir);  os.IsNotExist(err) {
					os.MkdirAll(dp.dstDir, 0755)
				}
				//copyFileAttribute(dp.dstDir, dp.srcDir)
				dp = doOneDirFileCopy(dp, fpList, dpChan)
				copyFileAttribute(dp.dstDir, dp.srcDir)
				logger.Printf("\t %s: finish copy '%s' to '%s'\n", taskId, dp.srcDir, dp.dstDir)
				logger.Printf("\t %s: dirs[%d] files[%d], totalSize[%d]bytes copyFiles[%d] totalCopySize[%d]bytes unsupport[%d] skip[%d] err[%d]\n", 
					taskId, dp.dirCount, dp.fileCount, dp.totalSize, dp.copyFileCount, dp.totalCopySize, dp.unsupportCount, dp.skipCount, dp.errCount)
			}()
		}
	}
       	go func() {
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
	}
        logger.Printf("\t Finished COPY ['%s'] to ['%s']\n", absSrcDir, absDstDir)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t Summary: Directorys: %d, Files: %d, allTotalSrcSize: %d bytes\n", allDirCount, allFileCount, allTotalSize)
        logger.Printf("\t          allCopyFileCount: %d, allTotalCopySize: %d bytes, allUnsupport: %d, allSkip: %d, allErr: %d\n",
			allCopyFileCount, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
        logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
        logger.Printf("\t ############################### END #############################################################\n\n\n")

}

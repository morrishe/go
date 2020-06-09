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
)

type DirNode struct {
	srcDir		string
	dstDir		string
	fileCount	int64
	dirCount	int64
	unsupportCount	int64
	skipCount	int64
	errCount	int64
	totalSrcSize	int64
	totalCopySize	int64
}

type FileNode struct {
	srcFile		string
	dstFile		string
	unsupport	bool
	skip		bool
	err		bool
	srcSize		int64
	copySize	int64
}

type FilePair struct {
	absSrcFile		string
	absDstFile		string
}

// log file, default '/tmp/nasCopy.log'
var logger	*log.Logger

func walkDir(dstDir string, srcDir string,  nDir *sync.WaitGroup, dirCh chan<- DirNode, dirSema chan struct{}, fileSema chan struct{}) {
        defer nDir.Done()

        dirSema <- struct{}{}
        defer func() { <-dirSema }()

	_, err := os.Lstat(srcDir)
	if os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Critital BUG: srcDir['%s'] is not exists", srcDir)
		os.Exit(2)
	}
	/* if dstDir isnot exists, create is */
	if _, err = os.Lstat(dstDir);  os.IsNotExist(err) {
		//Ignore error
		os.MkdirAll(dstDir, 0755)
	}
	// copy the directory attribute, and ignore error 
	copyFileAttribute(dstDir, srcDir)

	entrys := dirents(srcDir)
	var dirCount, fileCount, totalSrcSize	int64
	var nFile sync.WaitGroup
	var fp FilePair
	var fpList = make([]FilePair, 0)
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCount++
                        subSrcDir := filepath.Join(srcDir, entry.Name())
			subDstDir := filepath.Join(dstDir, entry.Name())
                        nDir.Add(1)
                        go walkDir(subDstDir, subSrcDir, nDir, dirCh, dirSema, fileSema)
                } else {
			fileCount++
			totalSrcSize += entry.Size()
			fp.absSrcFile = filepath.Join(srcDir, entry.Name())
			fp.absDstFile = filepath.Join(dstDir, entry.Name())
			fpList = append(fpList, fp)
		}
	}
	var mu sync.Mutex
	var fileChan = make(chan FileNode, DIRWORKERS)
	for _, fp = range fpList {
		nFile.Add(1)
		go doFileCopy(fp.absDstFile, fp.absSrcFile, fileCh, &nFile, fileSema)	
	}
	go func() {
		mu.Lock()
        	nFile.Wait()
		mu.Unlock()
	}

	var skipCount, errCount, unsupportCount, totalCopySize int64
	for fn := range fileChan {
		switch {
		case fn.skip:	skipCount++
		case fn.err:	errCount++	
		case fn.unsupport:
			unsupportCount++
		case fn.copySize:
			totalCopySize += fn.copySize
	}
			
		

	mu.Lock()

	var dn DirNode
	dn.srcDir = srcDir
	dn.dstDir = dstDir
	dn.dirCount = dirCount
	dn.fileCount = fileCount
	dn.totalSrcSize = totalSrcSize
        ch <- dn
}


func doFileCopy(dstFile string, srcFile string, nFile *sync.WaitGroup,  fileSema chan struct{}) {
	defer nFile.Done()

	fileSema <- struct{}{}
        defer func() { <-fileSema }()

	var sfi, dfi	os.FileInfo
	var err	error 
	var isSymlink, isRegular, isUnsupport	 bool

	if sfi, err = os.Lstat(srcFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "ERROR: %s, %v\n", srcFile, err)
		logger.Printf("\t Impossible!, BUG, Quit")
		os.Exit(2)
	}

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
		return
	}

	if isSymlink { //symblink file
		os.Remove(dstFile) // ignore error
		if link, err := os.Readlink(srcFile); err != nil {
			//ignore error
			//logger.Printf("\t os.Readlink('%s') error n", srcFile)
		} else {
			err = os.Symlink(link, dstFile)
			if err != nil {
				//ignore error
				//logger.Printf("\t os.Symlink('%s') error n", srcFile)
			}
		}
	}

	if isRegular {
		dfi, err = os.Lstat(dstFile)
		if os.IsNotExist(err) || dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
			// ModTime or Size is not same, file modified, copy it
			doRegularFileCopy(dstFile, srcFile)
		} else {
			//logger.Printf("\t %s exist and ModTime() and Size() is same, the same file, skip it\n", dstFile)
		}
	}
	copyFileAttribute(dstFile, srcFile)
}


func doRegularFileCopy(dstFile string, srcFile string) (int64, error) {
	var sf, df	*os.File
	var err		error 
	var writtenSize int64

	if sf, err = os.Open(srcFile); err != nil {
		fmt.Fprintf(os.Stderr, "open '%s' error: %v\n", srcFile, err)
		return 0, err 
	}
	defer sf.Close()
	if df, err = os.Create(dstFile); err != nil {
		fmt.Fprintf(os.Stderr, "open '%s' error: %v\n", dstFile, err)
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


/*
  This code exist a problem, get directory entry and release the lock
  and then doFileCopy(), it cost much time, and new goroutine can get the lock
  too many goroutine do fileCopy,  

//var sema = make(chan struct{}, 32)
func dirents(dir string, sema chan struct{}) []os.FileInfo {
        sema <- struct{}{}
        defer func() { <-sema }()

        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                log.Printf("\t %v\n", err)
                return nil
        }
        return entries
}
*/
func dirents(dir string) []os.FileInfo {
        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                log.Printf("\t %v\n", err)
                return nil
        }
        return entries
}

const (
	DIRWORKERS = 64
	FILEWORKERS = 1024
)

func main() {
	var dirWorkers	int
	var fileWorkers	int
	var logfile	string
	flag.IntVar(&dirWorkers, "walk directory workers", DIRWORKERS, "concurrent walk directory workers")
	flag.IntVar(&fileWorkers, "file copy workers", FILEWORKERS, "concurrent file copy workers")
	flag.StringVar(&logfile, "logfile", "/tmp/nasCopy.log", "log filename")

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
        dnChan := make(chan DirNode, dirWorker)
        var nDir sync.WaitGroup

	nDir.Add(1)
	go walkDir(absDstDir, absSrcDir, &nDir, dnChan, dirSema, fileSema)

        go func() {
                nDir.Wait()
                close(dnChan)
        }()

	var allTotalSrcSize, allTotalCopySize, allDirCount, allFileCount, allUnsupportCount, allSkipCount, allErrCount	int64
        for dn := range dnChan {
		allTotalSrcSize += dn.totalSrcSize
		allTotalCopySize += dn.totalCopySize
		allDirCount += 1
		allFileCount += dn.fileCount
		allUnsupportCount += dn.unsupportCount
		allSkipCount += dn.skipCount
		allErrCount += dn.errCount

		if (allDirCount % 1024 ==  0) {
			logger.Printf("\t Current chan: dnChan[%d]\n", len(dnChan))
			logger.Printf("\t Current progress: Directorys:[%d], Files: [%d]\n", allDirCount, allFileCount)
			logger.Printf("\t Current summary: allTotalSrcSize[%d], allTotalCopySize[%d], allUnsupport[%d], allSkip[%d], allErr[%d]\n",  
				allTotalSrcSize, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
		}
        }
	logger.Printf("\t Finished COPY ['%s'] to ['%s']\n", absSrcDir, absDstDir)
	logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
	logger.Printf("\t Summary: allFile[%d], allDir[%d], allTotalSrcSize[%d], allTotalCopySize[%d], allUnsupport[%d], allSkip[%d], allErr[%d]\n", allFileCount, allDirCount, 
				allTotalSrcSize, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
	logger.Printf("\t ----------------------------------------------------------------------------------------------------------------------------------------------------\n")
	logger.Printf("\t ############################### END #############################################################\n\n\n")

	fmt.Printf("Finished COPY ['%s'] to ['%s']\n", absSrcDir, absDstDir)
	fmt.Printf("----------------------------------------------------------------------------------------------------------------------------------------------------\n")
	fmt.Printf("Summary: allFile[%d], allDir[%d], allTotalSrcSize[%d], allTotalCopySize[%d], allUnsupport[%d], allSkip[%d], allErr[%d]\n", allFileCount, allDirCount, 
				allTotalSrcSize, allTotalCopySize, allUnsupportCount, allSkipCount, allErrCount)
	fmt.Printf("----------------------------------------------------------------------------------------------------------------------------------------------------\n")
}

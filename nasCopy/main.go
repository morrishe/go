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
	totalSize	int64
}

type FileNode struct {
	fileName	string
	size		int64
	isSymlink	bool
	isRegular	bool
	haveErr		bool
}

type FileEntry struct {
	absSrcName	string
	absDstName	string
}	

// log file, default '/tmp/nasCopy.log'
var logger	*log.Logger

// return os.FileInfo entry of directory
func dirents(dir string) []os.FileInfo {
        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                log.Printf("\t %v\n", err)
                return nil
        }
        return entries
}

func walkDir(dstDir string, srcDir string,  ndir *sync.WaitGroup, dirCh chan<- DirNode, fileCh chan<- FileEntry, sema chan struct{}) {
        defer ndir.Done()

	/* control concurrent walk directory count
	   default is 64
	*/
        sema <- struct{}{}
        defer func() { <-sema }()

	/* ignore error */
	os.MkdirAll(dstDir, 0755)
        copyEntryAttribute(dstDir, srcDir)

	entrys := dirents(srcDir)
	var dirCount, fileCount, totalSize 	int64
	var subDstDir, subSrcDir		string
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCount++
                        subSrcDir = filepath.Join(srcDir, entry.Name())
			subDstDir = filepath.Join(dstDir, entry.Name())
                        n.Add(1)
                        go walkDir(subDstDir, subSrcDir, n, dirCh, fileCh, sema)
                } else {
			fileCount++
			totalSize += entry.Size()
			var fe FileEntry
			fe.absSrcName = filepath.Join(srcDir, entry.Name())
			fe.absDstName = filepath.Join(dstDir, entry.Name())
			fileCh <- fe

		}
        }
	var dn DirNode
	dn.srcDir = subSrcDir
	dn.dstDir = subDstDir
	dn.dirCount = dirCount
	dn.fileCount = fileCount
	dn.totalSize = totalSize
        dirDh <- dn
}

func copyEntryAttribute(dst string, src string) error {
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
					logger.Printf("\t chmod(%s, %v) error\n", dst, mode)
					return e 
				}
				if e := os.Chtimes(dst, atime, mtime); e != nil {
					logger.Printf("\t os.Chtimes(%s, %v, %v) error\n", dst, atime, mtime)
					return e 
				}
			}
			if e := os.Lchown(dst, uid, gid); e != nil {
				logger.Printf("\t chown(%s, %d, %d) error\n", dst, uid, gid)
				return e 
			}
		}
	}
	return nil
}


func doFileCopy(dstFile string, srcFile string) (int64, bool, bool, error) {
	var sfi, dfi	os.FileInfo
	var err	error 
	var writtenSize int64
	var unsupport, skip bool

	sfi, err = os.Lstat(srcFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		logger.Printf("\t Impossible!, BUG, Quit")
		return 0, false, false, err
	}
	mode := sfi.Mode()
	switch {
	case mode&os.ModeNamedPipe != 0:
		logger.Printf("\t '%s' is NamedPipe file, unsupport!\n", srcFile)
		unsupport = true
	case mode&os.ModeSocket != 0:
		logger.Printf("\t '%s' is Socket file, unsupport!\n", srcFile)
		unsupport = true
	case mode&os.ModeDevice != 0:
		logger.Printf("\t '%s' is Device file, unsupport!\n", srcFile)
		unsupport = true
		return 0, true, false, nil
	case mode&os.ModeIrregular != 0:
		logger.Printf("\t '%s' is Irregular file, unsupport!\n", srcFile)
		unsupport = true
	}
	if unsupport {
		//return 0, unsupport, skip, nil
		return 0, unsupport, false, nil
	}

	if mode&os.ModeSymlink != 0 { //symblink file
		os.Remove(dstFile) // ignore error
		if link, err := os.Readlink(srcFile); err != nil {
			logger.Printf("\t os.Readlink('%s') error n", srcFile)
			return 0, false, false, err
		} else {
			err = os.Symlink(link, dstFile)
			if err != nil {
				logger.Printf("\t os.Symlink('%s') error n", srcFile)
			}
			return int64(len(link)), false, false, err 
		}
	}

	dfi, err = os.Lstat(dstFile)
	if os.IsNotExist(err) || dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
		// ModTime or Size is not same, file modified, copy it
		writtenSize, err = doRegularFileCopy(dstFile, srcFile)
	} else {
		//logger.Printf("\t %s exist and ModTime() and Size() is same, the same file, skip it\n", dstFile)
		skip = true
		return 0, false, skip, err
	}
	return  writtenSize, false, false, err
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


const (
	WALKGOROUTINE = 64
	FILEGOROUTINE = 512
	FILECHANLEN = 1024
)

func main() {
	var goWorker	int
	var logfile	string
	flag.IntVar(&dirWorkers, "dirgo", WALKGOROUTINE, "concurrent goroutine dir walk workers")
	flag.IntVar(&fileWorkers, "filego", FILEGOROUTINE, "concurrent goroutine file workers")
	flag.IntVar(&fileChanLen, "fileChan", FILECHANLEN, "concurrent goroutine file workers")
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


        FileEntryChan := make(chan FileNode, fileWorkers)
        var nfile sync.WaitGroup
        fileChan := make(chan FileNode, fileWorkers/2)

	dirSema := make(chan struct{}, dirWorkers)
        dirChan := make(chan DirNode, dirWorkers/2)
        var ndir sync.WaitGroup
	ndir.Add(1)
	go walkDir(absDstDir, absSrcDir, &ndir, dirChan, fileChan, dirSema)

        go func() {
                ndir.Wait()
                close(dirChan)
        }()

	nfile.Add(1)
	go doFileCopy(&nfile, FileChan, sema)
	go func() {
		nfile.Wait()
		close(fileChan)
	}

	var allTotalSrcSize, allTotalCopySize, allDirCount, allFileCount, allUnsupportCount, allSkipCount, allErrCount	int64
        for {
		select {
		case dn, dnOk := <-dirChan:

		case fn, fnOk := <-fileChan:

		default:
		}
		allTotalSrcSize += dn.totalSrcSize
		allTotalCopySize += dn.totalCopySize
		allDirCount += 1
		allFileCount += dn.fileCount
		allUnsupportCount += dn.unsupportCount
		allSkipCount += dn.skipCount
		allErrCount += dn.errCount
		// reduce print log
		if (allDirCount % 1024 ==  0) {
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

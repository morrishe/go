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
	name		string
	subFileCount	int64
	subDirCount	int64
	totalSize	int64
}

type FileNode struct {
	fileName	string
	size		int64
	copySize	int64
	isSymlink	bool
	isRegular	bool
	isUnsupport	bool
	haveErr		bool
}

type FileEntry struct {
	srcDir	string
	dstDir	string
	name	string
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

func walkDir(dstDir string, srcDir string,  ndir *sync.WaitGroup, dirCh chan<- DirNode, fileEntryCh chan FileEntry, sema chan struct{}) {
        defer ndir.Done()

	/* control concurrent walk directory count
	   default is 64
	*/
        sema <- struct{}{}
        defer func() { <-sema }()

	/* ignore error */
	//os.MkdirAll(dstDir, 0755)
        //copyEntryAttribute(dstDir, srcDir)

	entrys := dirents(srcDir)
	var dirCount, fileCount, totalSize 	int64
	var subDstDir, subSrcDir		string
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCount++
                        subSrcDir = filepath.Join(srcDir, entry.Name())
			subDstDir = filepath.Join(dstDir, entry.Name())
                        ndir.Add(1)
                        go walkDir(subDstDir, subSrcDir, ndir, dirCh, fileEntryCh, sema)
                } else {
			fileCount++
			totalSize += entry.Size()
			var fe FileEntry
			fe.name = entry.Name()
			fe.dstDir = dstDir
			fe.srcDir = srcDir
			fileEntryCh <- fe
		}
        }
	var dn DirNode
	dn.name = srcDir
	dn.subDirCount = dirCount
	dn.subFileCount = fileCount
	dn.totalSize = totalSize
        dirCh <- dn
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


func getFileAndCopy(nfile *sync.WaitGroup, fec chan FileEntry, fn chan<- FileNode, fileSema chan struct{}) {
	defer nfile.Done()


	for fe := range fec {
		absSrcFile := filepath.Join(fe.srcDir, fe.name)
		absDstFile := filepath.Join(fe.dstDir, fe.name)

		/* ignore error */
		os.MkdirAll(fe.dstDir, 0755)
        	copyEntryAttribute(fe.dstDir, fe.srcDir)

		nfile.Add(1)
		go doFileCopy(absDstFile, absSrcFile, nfile, fn, fileSema)
	}
}


func doFileCopy(dstFile string, srcFile string, nfile *sync.WaitGroup, fnChan chan<- FileNode, fileSema chan struct{}) {
	defer nfile.Done()

        fileSema <- struct{}{}
        defer func() { <-fileSema }()

	sfi, err := os.Lstat(srcFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\t Impossible!, BUG, Quit: %v\n", err)
		logger.Printf("\t Impossible!, BUG, Quit")
		os.Exit(2)
	}

	var isSymlink, isRegular, isUnsupport	bool
	var fn FileNode

	fn.fileName = srcFile

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
		fn.isUnsupport = true
		fn.haveErr = true
		fnChan <- fn
		return
	}

	fn.size = sfi.Size()

	if isSymlink {
		fn.isSymlink = true
		os.Remove(dstFile) // ignore error
		if link, err := os.Readlink(srcFile); err != nil {
			logger.Printf("\t os.Readlink('%s') error n", srcFile)
			fn.haveErr = true
		} else {
			err = os.Symlink(link, dstFile)
			if err != nil {
				logger.Printf("\t os.Symlink('%s') error n", srcFile)
				fn.haveErr = true
			} else { 
				fn.size = int64(len(link))
			}
		}
		fnChan <- fn
		return
	}

	if isRegular {
		dfi, err := os.Lstat(dstFile)
		if os.IsNotExist(err) || dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
			// ModTime or Size is not same, file modified, copy it
			wtSize, err := doRegularFileCopy(dstFile, srcFile)
			if err != nil {
				fn.haveErr = true
			} else {
				fn.copySize = wtSize
        			copyEntryAttribute(dstFile, srcFile)
			}
		}
		fnChan <- fn
	}
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
	DIRWORKERS = 64
	FILEWORKERS = 512
	FILECHANLEN = 1024
)

func main() {
	var dirWorkers, fileWorkers, fileChanLen	int
	var logfile	string

	flag.IntVar(&dirWorkers, "dirWorkers", DIRWORKERS, "concurrent goroutine dir walk workers")
	flag.IntVar(&fileWorkers, "fileWorkers", FILEWORKERS, "concurrent goroutine file workers")
	flag.IntVar(&fileChanLen, "fileChanLen", FILECHANLEN, "concurrent goroutine file workers")
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


        fileEntryChan := make(chan FileEntry, fileChanLen)
        fileChan := make(chan FileNode)
	fileSema := make(chan struct{}, fileWorkers)
        var nfile sync.WaitGroup

	dirSema := make(chan struct{}, dirWorkers)
        dirChan := make(chan DirNode, dirWorkers/2)
        var ndir sync.WaitGroup
	ndir.Add(1)
	go walkDir(absDstDir, absSrcDir, &ndir, dirChan, fileEntryChan, dirSema)
        go func() {
                ndir.Wait()
                close(dirChan)	// when walk around all directory and subdirectory, close the dirChan
		close(fileEntryChan)
        }()

	nfile.Add(1)
	go getFileAndCopy(&nfile, fileEntryChan, fileChan, fileSema)
	go func() {
		nfile.Wait()
		close(fileChan)
	}()

	var totalSize, totalCopySize, totalDirCount, totalFileCount	int64
	var dirChanClose, fileChanClose	bool
        for {
		select {
		case dn, dnOk := <-dirChan:
			if !dnOk {
				dirChanClose = true
			} else {
				totalDirCount++
				totalSize += dn.totalSize
				totalFileCount += dn.subFileCount
				logger.Printf("\t Todo: walk directorys[%d], filesize[%d], fileCount[%d] ", totalDirCount, totalSize, totalFileCount)
			}

		case fn, fnOk := <-fileChan:
			if !fnOk {
				fileChanClose = true
			} else {
				totalCopySize += fn.copySize
				logger.Printf("\t Current progress: CopySize[%d]bytes", totalCopySize)
			}
		}
		if (dirChanClose && fileChanClose) {
			break
		}
        }
	logger.Printf("\t Finished\n")
}

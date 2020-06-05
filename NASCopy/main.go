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

// log file, default '/tmp/nasCopy.log'
var logger	*log.Logger

func walkDir(dstDir string, srcDir string,  n *sync.WaitGroup, ch chan<- DirNode, sema chan struct{}) {
        defer n.Done()

	/* control concurrent walk directory count
	   default is 128 
	*/
        sema <- struct{}{}
        defer func() { <-sema }()

	var srcFi	os.FileInfo

	srcFi, err := os.Lstat(srcDir)
	if os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "srcDir['%s'] is not exists", srcDir)
		return
	} else {
		if _, err = os.Lstat(dstDir);  os.IsNotExist(err) {
			err := os.MkdirAll(dstDir, srcFi.Mode())
			if err != nil {
				fmt.Fprintf(os.Stderr, "MkdirAll(%s) error: %v", dstDir, err)
				logger.Printf("\t MkdirAll(%s) error: %v", dstDir, err)
				return
			}
		}
		// 
		e := copyFileAttribute(dstDir, srcDir)
		if (e != nil) {
			fmt.Fprintf(os.Stderr, "copyFileAttribute(%s, %s) error: %v\n", dstDir, srcDir, e)
			return
		}
	}

	entrys := dirents(srcDir)
	var dirCount, fileCount, totalSrcSize, totalCopySize, unsupportCount, skipCount, errCount	int64
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCount++
                        subSrcDir := filepath.Join(srcDir, entry.Name())
			subDstDir := filepath.Join(dstDir, entry.Name())
                        n.Add(1)
                        go walkDir(subDstDir, subSrcDir, n, ch, sema)
                } else {
			fileCount++
			totalSrcSize += entry.Size()
			srcFile := filepath.Join(srcDir, entry.Name())
			dstFile := filepath.Join(dstDir, entry.Name())
			size, unsupport, skip, err := doFileCopy(dstFile, srcFile)
			totalCopySize += size
			if err != nil {
				fmt.Fprintf(os.Stderr, "copy [%s] to [%s] occur error\n", srcFile, dstFile)
				logger.Printf("\t copy [%s] to [%s] occur error[%v]\n", srcFile, dstFile, err)
				errCount++
				continue
			} else if skip {
				skipCount++
			} else if unsupport {
				unsupportCount++
				continue
			}
			e := copyFileAttribute(dstFile, srcFile)
			if (e != nil) {
				fmt.Fprintf(os.Stderr, "copyFileAttribute(%s, %s) error: %v\n", dstFile, srcFile, e)
				log.Printf("\t copyFileAttribute(%s, %s) error: %v\n", dstFile, srcFile, e)
			}
		}
        }
	var dn DirNode
	dn.srcDir = srcDir
	dn.dstDir = dstDir
	dn.dirCount = dirCount
	dn.fileCount = fileCount
	dn.unsupportCount = unsupportCount
	dn.skipCount = skipCount
	dn.errCount = errCount
	dn.totalSrcSize = totalSrcSize
	dn.totalCopySize = totalCopySize
        ch <- dn
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
	GOROUTINEWORKER = 128
)

func main() {
	var goWorker	int
	var logfile	string
	flag.IntVar(&goWorker, "worker", GOROUTINEWORKER, "concurrent goroutine worker")
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

	sema := make(chan struct{}, goWorker)
        dnChan := make(chan DirNode)
        var n sync.WaitGroup

	n.Add(1)
	go walkDir(absDstDir, absSrcDir, &n, dnChan, sema)

        go func() {
                n.Wait()
                close(dnChan)
        }()

	var allTotalSrcSize, allTotalCopySize, allDirCount, allFileCount, allUnsupportCount, allSkipCount, allErrCount	int64
        for dn := range dnChan {
		allTotalSrcSize += dn.totalSrcSize
		allTotalCopySize += dn.totalCopySize
		allDirCount += dn.dirCount
		allFileCount += dn.fileCount
		allUnsupportCount += dn.unsupportCount
		allSkipCount += dn.skipCount
		allErrCount += dn.errCount
		if dn.fileCount > 0 {
			logger.Printf("\t Finish copy Directory['%s'] to ['%s']\n", dn.srcDir, dn.dstDir)
			logger.Printf("\t\tSummary: File[%d], Dir[%d], TotalSrcSize[%d], TotalCopySize[%d], unsupport[%d], skip[%d], err[%d]\n", dn.fileCount, dn.dirCount, 
					dn.totalSrcSize, dn.totalCopySize, dn.unsupportCount, dn.skipCount, dn.errCount)
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

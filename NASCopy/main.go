package main

import (
	"os"
	"fmt"
	//"strings"
	"flag"
	"log"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	//"time"
)


type DirNode struct {
	srcDir		string
	dstDir		string
	fileCount	int64
	dirCount	int64
	totalSize	int64
	errExist	bool
}

// log file, default '/tmp/NASCopy.log'
var logger	*log.Logger

func copyDir(dstDir string, srcDir string,  n *sync.WaitGroup, ch chan<- DirNode, sema chan struct{}) {
        defer n.Done()
	var srcFi	os.FileInfo

	srcFi, err := os.Lstat(srcDir)
	if os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "srcDir['%s'] is not exists", srcDir)
		os.Exit(2)	
	} else {
		if _, err = os.Lstat(dstDir);  os.IsNotExist(err) {
			err := os.MkdirAll(dstDir, srcFi.Mode())
			if err != nil {
				fmt.Fprintf(os.Stderr, "MkdirAll(%s) error: %v", dstDir, err)
				os.Exit(2)
			}
		}
	}

	entrys := dirents(srcDir, sema)
	var dirCount, fileCount, totalSize	int64
	var errExist	bool
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCount++
                        n.Add(1)
                        subSrcDir := filepath.Join(srcDir, entry.Name())
			subDstDir := filepath.Join(dstDir, entry.Name())
                        go copyDir(subDstDir, subSrcDir, n, ch, sema)
                } else {
			fileCount++
			totalSize += entry.Size()
			srcFile := filepath.Join(srcDir, entry.Name())
			dstFile := filepath.Join(dstDir, entry.Name())
			_, err := do_copy(dstFile, srcFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "copy [%s] to [%s] occur error\n", srcFile, dstFile)
				errExist = true
				continue
			}
		}
        }
	var dn DirNode
	dn.srcDir = srcDir
	dn.dstDir = dstDir
	dn.dirCount = dirCount
	dn.fileCount = fileCount
	dn.totalSize = totalSize
	dn.errExist = errExist
	
        ch <- dn
}


func do_copy(dstFile string, srcFile string) (int64, error) {
	var sfi, dfi	os.FileInfo
	var sf, df	*os.File
	var err	error 
	var writtenSize int64

	sfi, err = os.Lstat(srcFile)
	dfi, err = os.Lstat(dstFile)
	if os.IsNotExist(err) || dfi.ModTime() != sfi.ModTime() || dfi.Size() != sfi.Size() {
		logger.Printf("\t copy '%s' to '%s'\n", srcFile, dstFile)	
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

	} else {
		fmt.Fprintf(os.Stderr, "%s exist, skip it\n", dstFile)
		return 0, err
	}
	
	return  writtenSize, err

}


//var sema = make(chan struct{}, 32)
func dirents(dir string, sema chan struct{}) []os.FileInfo {
        sema <- struct{}{}
        defer func() { <-sema }()

        entries, err := ioutil.ReadDir(dir)
        if err != nil {
                log.Printf("%v\n", err)
                return nil
        }
        return entries
}



const (
	GOROUTINEWORKER = 8
)

func main() {
	var goWorker	int
	var logfile	string
	flag.IntVar(&goWorker, "gol", GOROUTINEWORKER, "concurrent goroutine worker")
	flag.StringVar(&logfile, "logfile", "/tmp/NASCopy.log", "log filename")

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

	logger.Printf("\t Begin to COPY ['%s'] to ['%s'].....\n", absSrcDir, absDstDir)

	sema := make(chan struct{}, goWorker)
        dnChan := make(chan DirNode)
        var n sync.WaitGroup

	n.Add(1)
	go copyDir(absDstDir, absSrcDir, &n, dnChan, sema)

        go func() {
                n.Wait()
                close(dnChan)
        }()

        for dn := range dnChan {
		logger.Printf("\t Directory: %s, Total size: %d\n", dn.srcDir, dn.totalSize)
        }
	logger.Printf("\t Finished COPY ['%s'] to ['%s'].....\n", absSrcDir, absDstDir)
	logger.Printf("\n\n\n")
}

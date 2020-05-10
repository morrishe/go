package main

import (
	"os"
	"fmt"
	"strings"
	"flag"
	"log"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"
)


type DirNode struct {
	FileCounts	int
	DirCounts	int
	Depth		int
	PathName	string
}

func walkDir(dir string, depth int,  n *sync.WaitGroup, ch chan<- DirNode, sema chan struct{}) {
        defer n.Done()
	entrys := dirents(dir, sema)
	var dirCounts, fileCounts int
        for _, entry := range entrys {
                if entry.IsDir() {
			dirCounts++
                        n.Add(1)
                        subdir := filepath.Join(dir, entry.Name())
                        go walkDir(subdir, depth+1, n, ch, sema)
                } else {
			fileCounts++
		}
        }
	var dn DirNode
	dn.FileCounts = fileCounts
	dn.DirCounts = dirCounts
	dn.PathName = dir
	dn.Depth = depth
        ch <- dn
}

//var sema = make(chan struct{}, 32)
var workers int
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
	ENTRYLIMIT = 512 * 1024
	DEPTHLIMIT = 20
	NAMELENLIMIT = 128
	OUTPUTFILE = "/tmp/NasWalk.log"
	GOROUTINEWORKER = 16 
)

func main() {
	var entryLimit,depthLimit,goWorker,namelenLimit int
	var output string
	flag.IntVar(&entryLimit, "el", ENTRYLIMIT, "maxinum directory entrys")
	flag.IntVar(&depthLimit, "dl", DEPTHLIMIT, "maxinum directory depth")
	flag.IntVar(&goWorker, "gol", GOROUTINEWORKER, "concurrent goroutine worker")
	flag.IntVar(&namelenLimit, "nl", NAMELENLIMIT, "maxinum file name length")
	flag.StringVar(&output, "output", "/tmp/nasWalk.output", "output filename")

        flag.Parse()
        roots := flag.Args()
        if len(roots) != 1 {
		fmt.Fprintf(os.Stderr, "USAGE: %s: [options] dir\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

	f, err := os.OpenFile(output, os.O_APPEND | os.O_RDWR | os.O_CREATE, 0755)
        if err != nil {
                log.Fatal(err)
        }
        defer f.Close()

	sema := make(chan struct{}, goWorker)
        dnChan := make(chan DirNode)
        var n sync.WaitGroup

	fmt.Println("Begin work ......")
	fmt.Printf("goroutine[%d], entryLimit[%d], depthLimit[%d], namelenLimit[%d], output:[%s]\n", goWorker, entryLimit, depthLimit, namelenLimit, output)

        for _, root := range roots {
                n.Add(1)
		absRoot, err := filepath.Abs(root)
		if err != nil {
			log.Println(err)
			continue
		}
		depth := strings.Count(absRoot, "/")
                go walkDir(absRoot, depth, &n, dnChan, sema)
        }

        go func() {
                n.Wait()
                close(dnChan)
        }()

	var maxEntry, maxDepth, maxNameLen	int
	var maxdn	DirNode
	var exceeded	bool
	
	//var loops int
        for dn := range dnChan {
		//loops++
		//fmt.Println(loops)
		//fmt.Printf("DIR:[%s]: depth[%d], files[%d], dirs[%d]\n", dn.PathName, dn.Depth, dn.FileCounts, dn.DirCounts)
		if dn.Depth > maxDepth {
			maxDepth = dn.Depth
			if maxDepth > depthLimit {
				f.WriteString(time.Now().String()[0:19])
				f.WriteString(fmt.Sprintf("    *** %s: depth[%d] EXCEED limit[%d], entrys[%d], files[%d], dirs[%d]\n", dn.PathName, dn.Depth, depthLimit,
					dn.FileCounts+dn.DirCounts, dn.FileCounts, dn.DirCounts))
				exceeded = true
				break
			}
			maxdn = dn
		} else if dn.FileCounts + dn.DirCounts > maxEntry {
			maxEntry = dn.FileCounts + dn.DirCounts
			if maxEntry > entryLimit {
				f.WriteString(time.Now().String()[0:19])
				f.WriteString(fmt.Sprintf("    *** %s: depth[%d], entrys[%d] EXCEED limit[%d], files[%d], dirs[%d]\n", dn.PathName, dn.Depth, 
					dn.FileCounts+dn.DirCounts, entryLimit, dn.FileCounts, dn.DirCounts))
				exceeded = true
				break
			}
			maxdn = dn
		} else if len(filepath.Base(dn.PathName)) > maxNameLen {
			maxNameLen = len(filepath.Base(dn.PathName))
			if maxNameLen > namelenLimit {
				f.WriteString(time.Now().String()[0:19])
				f.WriteString(fmt.Sprintf("    *** %s: depth[%d], entrys[%d],  files[%d], dirs[%d], filenameLenth:[%d] EXCEED limit[%d]\n", dn.PathName, dn.Depth, 
					dn.FileCounts+dn.DirCounts, dn.FileCounts, dn.DirCounts, maxNameLen, namelenLimit))
			}
			maxdn = dn
		}
			
        }
	if exceeded == true {
		fmt.Println("*** Break *** ")
		f.Close()
		os.Exit(2)
	}
	f.WriteString(time.Now().String()[0:19])
	f.WriteString(fmt.Sprintf("    %s: depth[%d], entrys[%d],  files[%d], dirs[%d]\n", maxdn.PathName, maxdn.Depth, 
		maxdn.FileCounts+maxdn.DirCounts, maxdn.FileCounts, maxdn.DirCounts))
	fmt.Println("Finished!")
}

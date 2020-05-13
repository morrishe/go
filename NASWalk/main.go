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
	maxNameLenFile	string
	maxNameLen	int
}

func walkDir(dir string, depth int,  n *sync.WaitGroup, ch chan<- DirNode, sema chan struct{}) {
        defer n.Done()
	entrys := dirents(dir, sema)
	var dirCounts, fileCounts, maxNameLen int
	var maxNameLenFile string
        for _, entry := range entrys {
		if len(entry.Name()) > maxNameLen {
			maxNameLen = len(entry.Name())
			maxNameLenFile = entry.Name()
		}
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
	dn.maxNameLenFile = maxNameLenFile
	dn.maxNameLen = maxNameLen
	
        ch <- dn
}

//var sema = make(chan struct{}, 32)
//var workers int
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
		fmt.Fprintf(os.Stderr, "USAGE: %s [options] dir\n", os.Args[0])
    		flag.PrintDefaults()
		os.Exit(1)
        }

	//Only walk a top-level directory for simple
	root := roots[0]
	absRoot, err := filepath.Abs(root)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(absRoot); os.IsNotExist(err) {
		fmt.Printf("ERROR: %s is not exists, Quit!\n", absRoot)
		os.Exit(2)
	}

	f, err := os.OpenFile(output, os.O_APPEND | os.O_RDWR | os.O_CREATE, 0755)
        if err != nil {
                log.Fatal(err)
        }
        defer f.Close()

	f.WriteString(time.Now().String()[0:19])
	f.WriteString(fmt.Sprintf("     Begin walk directory: [%s] ......\n", absRoot))
	f.WriteString(fmt.Sprintf("                        Current parameters: goroutine[%d], entryLimit[%d], depthLimit[%d], namelenLimit[%d], output:[%s]\n", 
			goWorker, entryLimit, depthLimit, namelenLimit, output))

	sema := make(chan struct{}, goWorker)
        dnChan := make(chan DirNode)
        var n sync.WaitGroup

	n.Add(1)
	depth := strings.Count(absRoot, "/")
	go walkDir(absRoot, depth, &n, dnChan, sema)

        go func() {
                n.Wait()
                close(dnChan)
        }()

	var maxEntry, maxDepth, maxNameLen		int
	var maxDepthDN, maxEntryDN, maxNameLenDN	DirNode
	var depthExceeded, entryExceeded  		bool
	
        for dn := range dnChan {
		if dn.Depth > maxDepth {
			maxDepth = dn.Depth
			if maxDepth > depthLimit {
				f.WriteString(time.Now().String()[0:19])
				f.WriteString(fmt.Sprintf("     *** depth EXCEEDS ***  Quit walk......\n"))
				depthExceeded = true
				maxDepthDN = dn
				break
			}
			maxDepthDN = dn
		} 
		if dn.FileCounts + dn.DirCounts > maxEntry {
			maxEntry = dn.FileCounts + dn.DirCounts
			if maxEntry > entryLimit {
				f.WriteString(time.Now().String()[0:19])
				f.WriteString(fmt.Sprintf("     *** entrys EXCEEDS ***   Quit walk......\n"))
				entryExceeded = true
				maxEntryDN = dn
				break
			}
			maxEntryDN = dn
		} 
		if dn.maxNameLen > maxNameLen {
			maxNameLen = dn.maxNameLen
			if maxNameLen > namelenLimit {
				f.WriteString(time.Now().String()[0:19])
				f.WriteString(fmt.Sprintf("     *** filenameLen EXCEEDS *** , Continue walk......\n"))
				f.WriteString(fmt.Sprintf("                       %s: filenameLenth:[%d] EXCEED limit[%d]\n", 
					filepath.Join(dn.PathName, dn.maxNameLenFile), maxNameLen, namelenLimit))
			}
			maxNameLenDN = dn
		}
			
        }

	f.WriteString(fmt.Sprintf("                        ------------------------ RESULT ----------------------------\n"))
	if !depthExceeded && !entryExceeded {
		f.WriteString(fmt.Sprintf("                        maxDepth: [%s]: depth[%d], entrys[%d],  files[%d], dirs[%d]\n", maxDepthDN.PathName, maxDepthDN.Depth, 
			maxDepthDN.FileCounts+maxDepthDN.DirCounts, maxDepthDN.FileCounts, maxDepthDN.DirCounts))
		f.WriteString(fmt.Sprintf("                        maxEntry: [%s]: depth[%d], entrys[%d],  files[%d], dirs[%d]\n", maxEntryDN.PathName, maxEntryDN.Depth, 
			maxEntryDN.FileCounts+maxEntryDN.DirCounts, maxEntryDN.FileCounts, maxEntryDN.DirCounts))
		f.WriteString(fmt.Sprintf("                        maxNameLen: [%s] length[%d]\n", filepath.Join(maxNameLenDN.PathName, maxNameLenDN.maxNameLenFile), maxNameLenDN.maxNameLen))
	} else if depthExceeded {
		f.WriteString(fmt.Sprintf("                        maxDepth: [%s]: depth[%d], entrys[%d],  files[%d], dirs[%d]\n", maxDepthDN.PathName, maxDepthDN.Depth, 
			maxDepthDN.FileCounts+maxDepthDN.DirCounts, maxDepthDN.FileCounts, maxDepthDN.DirCounts))
	} else if entryExceeded {
		f.WriteString(fmt.Sprintf("                        maxEntry: [%s]: depth[%d], entrys[%d],  files[%d], dirs[%d]\n", maxEntryDN.PathName, maxEntryDN.Depth, 
			maxEntryDN.FileCounts+maxEntryDN.DirCounts, maxEntryDN.FileCounts, maxEntryDN.DirCounts))
	}
	f.WriteString(fmt.Sprintf("                        ------------------------ End of RESULT ---------------------\n"))

	if depthExceeded == true {
		f.WriteString(time.Now().String()[0:19])
		f.WriteString(fmt.Sprintf("     *** Interrupt walk [%s]!!!  because of 'Depth' EXCEED limit ***\n\n\n", absRoot))
	} else if entryExceeded == true {
		f.WriteString(time.Now().String()[0:19])
		f.WriteString(fmt.Sprintf("     *** Interrupt walk [%s]!!!  because of 'Entry' EXCEED limit ***\n\n\n", absRoot))
	} else {
		f.WriteString(time.Now().String()[0:19])
		f.WriteString(fmt.Sprintf("     Finished walk [%s], Everything is OK!!!\n\n\n", absRoot))
	}
}

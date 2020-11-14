package main
import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

type respData struct {
	resp	*http.Response
	err	error
}

func doCall(ctx context.Context) {
	transport := http.Transport {
		DisableKeepAlives: true,
	}
	client := http.Client {
		Transport: &transport,
	}

	respChan := make(chan *respData, 1)
	req, err := http.NewRequest("GET", "http://127.0.0.1:8000/", nil)
	if err != nil {
		fmt.Printf("new request failed, err: %v\n", err)
		return
	}	

	req = req.WithContext(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		resp, err := client.Do(req)
		fmt.Printf("client.do resp: %v, err: %v\n", resp, err)
		rd := &respData {
			resp: resp,
			err: err,
		}
		respChan <- rd
		wg.Done()
	} ()
	
	select {
	case <- ctx.Done():
		fmt.Println("call api timeout")
	case result := <- respChan:
		fmt.Println("call server api success")
		if result.err != nil {
			fmt.Printf("call server api failed, err: %v\n", result.err)
			return
		}
		defer result.resp.Body.Close()
		data, _ := ioutil.ReadAll(result.resp.Body)
		fmt.Printf("resp:%v\n", string(data))
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	doCall(ctx)
}

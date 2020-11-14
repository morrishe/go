package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
)

func main() {
	transport := http.Transport {
		DisableKeepAlives: true,
	}
	client := http.Client {
		Transport: &transport,
	}

	req, err := http.NewRequest("GET", "http://www.sina.com.cn/index.html", nil)
	if err != nil {
		fmt.Printf("new request falied, err:%v\n", err)
		return
	}

	resp, err := client.Do(req)
	if (err != nil) {
		fmt.Printf("client.Do failed, error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	data, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("%s", data)
}

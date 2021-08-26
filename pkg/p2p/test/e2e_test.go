/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"testing"

	"github.com/alibaba/accelerated-container-image/pkg/p2p/fs"
	"github.com/alibaba/accelerated-container-image/pkg/p2p/util"

	"github.com/stretchr/testify/assert"
)

func testGet(t *testing.T, proxyClient *http.Client, reqURL string) {
	t.Helper()
	Assert := assert.New(t)
	proxyRes, err := proxyClient.Get(reqURL)
	if err != nil {
		t.Fatalf("Proxy client get %s fail! %s", reqURL, err)
	}
	parseURL, err := url.Parse(reqURL)
	if err != nil {
		t.Fatalf("Parser url %s fail! %s", reqURL, err)
	}
	proxyBody, _ := ioutil.ReadAll(proxyRes.Body)
	r, hit := requestMap.Load(parseURL.Path)
	if !hit {
		t.Fatal("Fatal!")
	}
	body := []byte(r.(string))
	if Assert.Equal(len(body), len(proxyBody)) {
		checkLen := fs.Min(100, len(body))
		Assert.Equal(body[:checkLen], proxyBody[:checkLen])
		Assert.Equal(body[len(body)-checkLen:], proxyBody[len(body)-checkLen:])
	}
}

var transportMap = &sync.Map{}

func getClient(t *testing.T, proxyAddress string) *http.Client {
	proxyURL, err := url.Parse(fmt.Sprintf("http://%s", proxyAddress))
	if err != nil {
		t.Fatalf("Proxy url parse fail! %s", err)
	}
	proxyTransport, _ := transportMap.LoadOrStore(proxyAddress, &http.Transport{
		Proxy:           http.ProxyURL(proxyURL),
		MaxConnsPerHost: 100,
	})
	return &http.Client{
		Transport: proxyTransport.(*http.Transport),
	}
}

func testProxy(t *testing.T, serverList, httpServerList, httpsServerList []*http.Server) {
	for i := 0; i < 10; i++ {
		filename := util.GetRandomString(64)
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func(filename string) {
				defer wg.Done()
				proxyAddress := serverList[rand.Intn(len(serverList))].Addr
				httpAddress := httpServerList[rand.Intn(len(httpServerList))].Addr
				httpsAddress := httpsServerList[rand.Intn(len(httpsServerList))].Addr
				proxyClient := getClient(t, proxyAddress)
				if rand.Int()%2 == 0 {
					// redirect
					if rand.Int()%2 == 0 {
						testGet(t, proxyClient, fmt.Sprintf("https://127.0.0.1%s/%s", httpsAddress, filename))
					} else {
						testGet(t, proxyClient, fmt.Sprintf("http://127.0.0.1%s/%s", httpAddress, filename))
					}
				} else {
					// p2p
					if rand.Int()%2 == 0 {
						testGet(t, proxyClient, fmt.Sprintf("https://127.0.0.1%s/v2/blobs/sha256/%s/data", httpsAddress, filename))
					} else {
						testGet(t, proxyClient, fmt.Sprintf("http://127.0.0.1%s/v2/blobs/sha256/%s/data", httpAddress, filename))
					}
				}
			}(filename)
		}
	}
	wg.Wait()
}

type TestMatrix struct {
	root, agent            int
	serveBySSL, proxyHTTPS bool
}

func TestE2E(t *testing.T) {
	// TODO support serveBySSL
	testMatrix := []TestMatrix{
		{1, 3, false, false},
		{1, 3, false, true},
	}
	httpServerList := startHTTPServers(t)
	httpsServerList := startHTTPSServers(t)
	for _, v := range testMatrix {
		// prepare
		serverList := startServers(t, v.root, v.agent, v.serveBySSL, v.proxyHTTPS)
		wg.Wait()
		// test
		testProxy(t, serverList, httpServerList, httpsServerList)
		// clean
		for _, server := range serverList {
			_ = server.Shutdown(context.TODO())
		}
		t.Log("Server shutdown success!")
		runtime.GC()
	}
	for _, server := range httpServerList {
		_ = server.Shutdown(context.TODO())
	}
	for _, server := range httpsServerList {
		_ = server.Shutdown(context.TODO())
	}
}

func TestDockerPull(t *testing.T) {
	Assert := assert.New(t)
	serverList := startServers(t, 1, 2, false, true)
	for _, server := range serverList {
		configureDocker(t, server.Addr)
		_, code := executeCmd(true, "docker", "pull", "wordpress")
		Assert.Equal(0, code)
		executeCmd(true, "docker", "rmi", "-f", "wordpress")
		executeCmd(true, "docker", "image", "prune", "-f")
	}
	for _, server := range serverList {
		_ = server.Shutdown(context.TODO())
	}
}

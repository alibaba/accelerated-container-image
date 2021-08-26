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
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alibaba/accelerated-container-image/pkg/p2p/configure"
	"github.com/alibaba/accelerated-container-image/pkg/p2p/server"
	"github.com/alibaba/accelerated-container-image/pkg/p2p/util"

	log "github.com/sirupsen/logrus"
)

const media = "/tmp/p2p-cache"

var (
	wg         sync.WaitGroup
	requestMap = &sync.Map{}
)

func setup() {
	rand.Seed(time.Now().UnixNano())
	log.SetLevel(log.InfoLevel)
	// ignore test.root
	flag.Bool("test.root", false, "")
}

func teardown() {
	if err := os.RemoveAll(media); err != nil {
		fmt.Printf("Remove %s failed! %s", media, err)
	}
}

func TestMain(m *testing.M) {
	log.Info("Start test!")
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func startServer(t *testing.T, idx int, runMode string, rootList []string, port int, serveBySSL, proxyHTTPS bool) *http.Server {
	var config = &configure.DeployConfig{}
	config.LogLevel = "debug"
	config.RunMode = runMode
	config.RootList = rootList
	config.NodeIP = "127.0.0.1"
	config.DetectAddr = ""
	config.ServeBySSL = serveBySSL
	config.Port = port
	config.CertConfig.CertEnable = proxyHTTPS
	config.CertConfig.GenerateCert = true
	config.CertConfig.CertPath = "/tmp/p2pcert.pem"
	config.CertConfig.KeyPath = "/tmp/p2pcert.key"
	config.CacheConfig.MemCacheEnable = true
	config.CacheConfig.MemCacheSize = 1 * 1024 * 1024 * 1024
	config.CacheConfig.FileCacheEnable = true
	config.CacheConfig.FileCacheSize = 1 * 1024 * 1024 * 1024
	config.CacheConfig.FileCachePath = fmt.Sprintf("/tmp/cache/%d", idx)
	config.PrefetchConfig.PrefetchEnable = true
	config.PrefetchConfig.PrefetchThread = 64
	configure.CheckConfig(config)
	proxyServer := server.Execute(config, false)
	go func() {
		if err := proxyServer.ListenAndServe(); err != nil {
			t.Logf("Server %s on port %d exit! %s", runMode, port, err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		address := fmt.Sprintf("127.0.0.1:%d", port)
		for !util.CheckTCPConn(address) {
			t.Logf("Check address %s failed! retry...", address)
			time.Sleep(time.Second)
		}
		t.Logf("Start %s on port %d success!", runMode, port)
	}()
	return proxyServer
}

func startServers(t *testing.T, root, agent int, serveBySSL, proxyHTTPS bool) []*http.Server {
	var res []*http.Server
	var rootList []string
	port := 19141
	for i := 0; i < root; i++ {
		res = append(res, startServer(t, port, "root", make([]string, 0), port, serveBySSL, proxyHTTPS))
		rootList = append(rootList, fmt.Sprintf("127.0.0.1:%d", port))
		port++
	}
	for i := 0; i < agent; i++ {
		tmp := make([]string, len(rootList))
		copy(tmp, rootList)
		res = append(res, startServer(t, port, "agent", tmp, port, serveBySSL, proxyHTTPS))
		port++
	}
	configureCA(t)
	return res
}

func getData() string {
	const blockSize = 1024 * 1024
	length := rand.Intn(4) * blockSize
	length += rand.Intn(blockSize)
	return util.GetRandomString(length)
}

// content-length support range
func httpHandle1(w http.ResponseWriter, r *http.Request) {
	url := fmt.Sprintf("%s://%s%s", r.URL.Scheme, r.URL.Host, r.URL.Path)
	log.Infof("HTTP server accept request %s %s", r.Method, url)
	// TODO support server fail
	//if rand.Intn(10) == 0 {
	//	return
	//}
	rr, _ := requestMap.LoadOrStore(r.URL.Path, getData())
	res := rr.(string)
	w.Header().Set("Accept-Ranges", "bytes")
	rangeStr := r.Header.Get("Range")
	if rangeStr != "" {
		rr := strings.Split(rangeStr, "=")
		rrr := strings.Split(rr[1], "-")
		left, _ := strconv.Atoi(rrr[0])
		right, _ := strconv.Atoi(rrr[1])
		w.Header().Set("Content-Length", fmt.Sprintf("%d", right-left+1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", left, right, len(res)))
		w.WriteHeader(206)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(res[left : right+1]))
		}
	} else {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(res)))
		w.WriteHeader(200)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(res))
		}
	}
}

func startHTTPServer(t *testing.T, port int, handle func(http.ResponseWriter, *http.Request)) *http.Server {
	addr := fmt.Sprintf(":%d", port)
	httpServer := &http.Server{Addr: addr, Handler: http.HandlerFunc(handle)}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			t.Logf("HTTP Server exit! %s", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		address := fmt.Sprintf("127.0.0.1:%d", port)
		for !util.CheckTCPConn(address) {
			t.Logf("Check address %s failed! retry...", address)
			time.Sleep(time.Second)
		}
		t.Log("Start HTTP Server success!")
	}()
	return httpServer
}

func startHTTPSServer(t *testing.T, port int, handle func(http.ResponseWriter, *http.Request)) *http.Server {
	server.GetRootCA("/tmp/p2pcert.pem", "/tmp/p2pcert.key", true)
	cert, key := server.GenerateCertificate("127.0.0.1")
	if err := ioutil.WriteFile("/tmp/httpcert.pem", cert, 0644); err != nil {
		t.Fatalf("Write file httpcert.pem failed! %s", err)
	}
	if err := ioutil.WriteFile("/tmp/httpcert.key", key, 0644); err != nil {
		t.Fatalf("Write file httpcert.key failed! %s", err)
	}
	addr := fmt.Sprintf(":%d", port)
	httpServer := &http.Server{Addr: addr, Handler: http.HandlerFunc(handle)}
	go func() {
		if err := httpServer.ListenAndServeTLS("/tmp/httpcert.pem", "/tmp/httpcert.key"); err != nil {
			t.Logf("HTTPS Server exit! %s", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		address := fmt.Sprintf("127.0.0.1:%d", port)
		for !util.CheckTCPConn(address) {
			t.Logf("Check address %s failed! retry...", address)
			time.Sleep(time.Second)
		}
		t.Log("Start HTTPS Server success!")
	}()
	return httpServer
}

func startHTTPServers(t *testing.T) []*http.Server {
	var res []*http.Server
	res = append(res, startHTTPServer(t, 19131, httpHandle1))
	return res
}

func startHTTPSServers(t *testing.T) []*http.Server {
	var res []*http.Server
	res = append(res, startHTTPSServer(t, 19132, httpHandle1))
	return res
}

func executeCmd(stdout bool, name string, cmd ...string) (string, int) {
	cl := exec.Command(name, cmd...)
	if stdout {
		cl.Stdout = os.Stdout
		cl.Stderr = os.Stderr
		_ = cl.Run()
		code := cl.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
		return "", code
	}
	r, _ := cl.Output()
	code := cl.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
	return string(r), code
}

func configureCA(t *testing.T) {
	res, _ := executeCmd(false, "cat", "/etc/issue")
	if strings.Contains(res, "Ubuntu") {
		// ubuntu
		executeCmd(true, "cp", "/tmp/p2pcert.pem", "/usr/local/share/ca-certificates/p2pcert.crt")
		executeCmd(true, "update-ca-certificates")
	} else if strings.Contains(res, "Kernel") {
		// Centos
		executeCmd(true, "cp", "/tmp/p2pcert.pem", "/etc/pki/ca-trust/source/anchors/p2pcert.crt")
		executeCmd(true, "update-ca-trust")
	} else {
		t.Fatalf("Unknown system!")
	}
}

func configureDocker(t *testing.T, proxyAddress string) {
	// proxy
	proxyAddress = fmt.Sprintf("http://127.0.0.1%s", proxyAddress)
	s := fmt.Sprintf("[Service]\nEnvironment=\"HTTP_PROXY=%s\"\nEnvironment=\"HTTPS_PROXY=%s\"", proxyAddress, proxyAddress)
	dir := "/etc/systemd/system/docker.service.d"
	fn := "/etc/systemd/system/docker.service.d/proxy.conf"
	if err := os.MkdirAll(dir, 0644); err != nil {
		t.Fatalf("Mkdir %s fail! %s", dir, err)
	}
	file, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open file %s fail! %s", fn, err)
	}
	if _, err := file.WriteString(s); err != nil {
		t.Fatalf("Write file %s fail! %s", fn, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close file %s fail! %s", fn, err)
	}
	// mirror
	//s = "{\"registry-mirrors\": [\"https://q5oyqv4y.mirror.aliyuncs.com\"]}"
	//dir = "/etc/docker"
	//fn = "/etc/docker/daemon.json"
	//if err := os.MkdirAll(dir, 0644); err != nil {
	//	t.Fatalf("Mkdir %s fail! %s", dir, err)
	//}
	//file, err = os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0644)
	//if err != nil {
	//	t.Fatalf("Open file %s fail! %s", fn, err)
	//}
	//if _, err := file.WriteString(s); err != nil {
	//	t.Fatalf("Write file %s fail! %s", fn, err)
	//}
	//if err := file.Close(); err != nil {
	//	t.Fatalf("Close file %s fail! %s", fn, err)
	//}
	// restart
	executeCmd(true, "systemctl", "daemon-reload")
	executeCmd(true, "systemctl", "restart", "docker")
}

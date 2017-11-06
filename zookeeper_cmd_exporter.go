package main

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	//"unicode"
	//"reflect"
	"strconv"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"flag"
)

var (
	listenAddress  = flag.String("web.listen-address", ":9079", "Address on which to expose metrics and web interface.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	zookeeperHost = flag.String("zookeeper-host", "localhost", "Zookeeper host address,default localhost.")
)

var (
	zk_avg_latency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_avg_latency",
		Help: "zk_avg_latency",
	})
	zk_max_latency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_max_latency",
		Help: "zk_max_latency",
	})
	zk_min_latency = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_min_latency",
		Help: "zk_min_latency",
	})

	zk_packets_received = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_packets_received",
		Help: "zk_packets_received",
	})
	zk_packets_sent = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_packets_sent",
		Help: "zk_packets_sent",
	})
	zk_num_alive_connections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_num_alive_connections",
		Help: "zk_num_alive_connections",
	})
	zk_outstanding_requests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_outstanding_requests",
		Help: "zk_outstanding_requests",
	})
	zk_znode_count = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_znode_count",
		Help: "zk_znode_count",
	})
	zk_watch_count = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_watch_count",
		Help: "zk_watch_count",
	})
	zk_ephemerals_count = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_ephemerals_count",
		Help: "zk_ephemerals_count",
	})
	zk_approximate_data_size = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_approximate_data_size",
		Help: "zk_approximate_data_size",
	})
	zk_open_file_descriptor_count = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_open_file_descriptor_count",
		Help: "zk_open_file_descriptor_count",
	})
	zk_max_file_descriptor_count = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zk_max_file_descriptor_count",
		Help: "zk_max_file_descriptor_count",
	})
)

func main() {
	var cmdStr = "echo  mntr|nc "+*zookeeperHost+" 2181"
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	//cmd := exec.Command("/bin/sh", "-c", `echo  mntr|nc cdhtest03 2181`)
	//cmd := exec.Command("cmd", "/C", `cd D:\\old && dir `)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("StdoutPipe: " + err.Error())
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Println("StderrPipe: ", err.Error())
		return
	}
	if err := cmd.Start(); err != nil {
		fmt.Println("Start: ", err.Error())
		return
	}
	bytesErr, err := ioutil.ReadAll(stderr)
	if err != nil {
		fmt.Println("ReadAll stderr: ", err.Error())
		return
	}
	if len(bytesErr) != 0 {
		fmt.Printf("stderr is not nil: %s", bytesErr)
		return
	}
	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		fmt.Println("ReadAll stdout: ", err.Error())
		return
	}
	if err := cmd.Wait(); err != nil {
		fmt.Println("Wait: ", err.Error())
		return
	}
	println(len(string(bytes)))
	var str = string(bytes)
	//var str = "zk_avg_latency\t8237\n zk_max_latency\t151\n zk_min_latency\t0"
	split := strings.Split(str, "\n")
	for _,value := range split {
		line := strings.Split(value, "\t")
		if len(line) == 2 {
			key := strings.TrimSpace(line[0])
			value := strings.TrimSpace(line[1])
			//_, err := strconv.ParseInt(value, 10, 64)
			v, err := strconv.ParseFloat(value, 64)
			//println(key+"::"+value)
			if (err != nil) {
			} else {
				if key == "zk_avg_latency" {
					zk_avg_latency.Set(v)
					prometheus.MustRegister(zk_avg_latency)
				} else if key == "zk_max_latency" {
					zk_max_latency.Set(v)
					prometheus.MustRegister(zk_max_latency)
				} else if key == "zk_min_latency" {
					zk_min_latency.Set(v)
					prometheus.MustRegister(zk_min_latency)
				} else if key == "zk_packets_received" {
					zk_packets_received.Set(v)
					prometheus.MustRegister(zk_packets_received)
				} else if key == "zk_packets_sent" {
					zk_packets_sent.Set(v)
					prometheus.MustRegister(zk_packets_sent)
				} else if key == "zk_num_alive_connections" {
					zk_num_alive_connections.Set(v)
					prometheus.MustRegister(zk_num_alive_connections)
				} else if key == "zk_outstanding_requests" {
					zk_outstanding_requests.Set(v)
					prometheus.MustRegister(zk_outstanding_requests)
				} else if key == "zk_znode_count" {
					zk_znode_count.Set(v)
					prometheus.MustRegister(zk_znode_count)
				} else if key == "zk_watch_count" {
					zk_watch_count.Set(v)
					prometheus.MustRegister(zk_watch_count)
				} else if key == "zk_ephemerals_count" {
					zk_ephemerals_count.Set(v)
					prometheus.MustRegister(zk_ephemerals_count)
				} else if key == "zk_approximate_data_size" {
					zk_approximate_data_size.Set(v)
					prometheus.MustRegister(zk_approximate_data_size)
				} else if key == "zk_open_file_descriptor_count" {
					zk_open_file_descriptor_count.Set(v)
					prometheus.MustRegister(zk_open_file_descriptor_count)
				} else if key == "zk_max_file_descriptor_count" {
					zk_max_file_descriptor_count.Set(v)
					prometheus.MustRegister(zk_max_file_descriptor_count)
				}
				//zkGauge := prometheus.NewGauge(prometheus.GaugeOpts{
				//	Name: key,
				//	Help: key,
				//})
				//println(key)
				//zkAvgLatency.Set(v)
				//prometheus.MustRegister(zkGauge)
			}
		}
	}

	fmt.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>Zookeeper Exporter</title></head>
		<body>
		<h1>zookeeper Exporter</h1>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>`))
	})

	errHttp := http.ListenAndServe(*listenAddress, nil)
	if errHttp != nil {
		fmt.Errorf("erro:", errHttp)
	}
}

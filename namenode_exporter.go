package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	//"github.com/prometheus/log"
	"fmt"
)

const (
	namespace = "namenode"
)

var (
	listenAddress  = flag.String("web.listen-address", ":9070", "Address on which to expose metrics and web interface.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	//namenodeJmxUrl = flag.String("namenode.jmx.url", "http://localhost:50070/jmx", "Hadoop JMX URL.")
	namenodeJmxUrl = flag.String("namenode.jmx.url", "http://hadoop03:50070/jmx", "Hadoop JMX URL.")
)

type Exporter struct {
	url                      string
	//Hadoop:service=NameNode,name=FSNamesystem
	MissingBlocks            prometheus.Gauge
	CapacityTotal            prometheus.Gauge
	CapacityUsed             prometheus.Gauge
	CapacityRemaining        prometheus.Gauge
	CapacityUsedNonDFS       prometheus.Gauge
	BlocksTotal              prometheus.Gauge
	FilesTotal               prometheus.Gauge
	CorruptBlocks            prometheus.Gauge
	ExcessBlocks             prometheus.Gauge
	StaleDataNodes           prometheus.Gauge
	TotalLoad                prometheus.Gauge
	ScheduledReplicationBlocks         prometheus.Gauge
	PendingReplicationBlocks           prometheus.Gauge
        //java.lang:type=GarbageCollector,name=ParNew
	pnGcCount                prometheus.Counter
	pnGcTime                 prometheus.Counter
	//java.lang:type=GarbageCollector,name=ConcurrentMarkSweep
	cmsGcCount               prometheus.Counter
	cmsGcTime                prometheus.Counter
	//java.lang:type=Memory
	heapMemoryUsageCommitted prometheus.Gauge
	heapMemoryUsageInit      prometheus.Gauge
	heapMemoryUsageMax       prometheus.Gauge
	heapMemoryUsageUsed      prometheus.Gauge
	//Hadoop:service=NameNode,name=FSNamesystemState
	VolumeFailuresTotal           prometheus.Gauge
	EstimatedCapacityLostTotal           prometheus.Gauge
	//Hadoop:service=NameNode,name=NameNodeActivity
	TotalFileOps prometheus.Gauge
	GetBlockLocations prometheus.Gauge
	FilesCreated prometheus.Gauge
	CreateFileOps prometheus.Gauge
	CacheReportNumOps prometheus.Gauge
	CacheReportAvgTime prometheus.Gauge
	BlockReportNumOps prometheus.Gauge
	BlockReportAvgTime prometheus.Gauge
	AddBlockOps prometheus.Gauge
	//Hadoop:service=NameNode,name=JvmMetrics
	GcTimeMillis prometheus.Gauge
	GcTimeMillisParNew prometheus.Gauge
	GcTimeMillisConcurrentMarkSweep prometheus.Gauge
	GcCount prometheus.Gauge
	GcCountParNew prometheus.Gauge
	GcCountConcurrentMarkSweep prometheus.Gauge
	ThreadsBlocked prometheus.Gauge
	//Hadoop:service=NameNode,name=RpcDetailedActivityForPort8020
	GetListingAvgTime prometheus.Gauge
	GetFileInfoAvgTime prometheus.Gauge
}

func NewExporter(url string) *Exporter {
	return &Exporter{
		url: url,
		MissingBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "MissingBlocks",
			Help:      "MissingBlocks",
		}),
		CapacityTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityTotal",
			Help:      "CapacityTotal",
		}),
		CapacityUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityUsed",
			Help:      "CapacityUsed",
		}),
		CapacityRemaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityRemaining",
			Help:      "CapacityRemaining",
		}),
		CapacityUsedNonDFS: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CapacityUsedNonDFS",
			Help:      "CapacityUsedNonDFS",
		}),
		BlocksTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BlocksTotal",
			Help:      "BlocksTotal",
		}),
		FilesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "FilesTotal",
			Help:      "FilesTotal",
		}),
		CorruptBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CorruptBlocks",
			Help:      "CorruptBlocks",
		}),
		ExcessBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ExcessBlocks",
			Help:      "ExcessBlocks",
		}),
		StaleDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "StaleDataNodes",
			Help:      "StaleDataNodes",
		}),
		TotalLoad: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "TotalLoad",
			Help:      "TotalLoad",
		}),
		ScheduledReplicationBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ScheduledReplicationBlocks",
			Help:      "ScheduledReplicationBlocks",
		}),
		PendingReplicationBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "PendingReplicationBlocks",
			Help:      "PendingReplicationBlocks",
		}),
		pnGcCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ParNew_CollectionCount",
			Help:      "ParNew GC Count",
		}),
		pnGcTime: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ParNew_CollectionTime",
			Help:      "ParNew GC Time",
		}),
		cmsGcCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ConcurrentMarkSweep_CollectionCount",
			Help:      "ConcurrentMarkSweep GC Count",
		}),
		cmsGcTime: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ConcurrentMarkSweep_CollectionTime",
			Help:      "ConcurrentMarkSweep GC Time",
		}),
		heapMemoryUsageCommitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageCommitted",
			Help:      "heapMemoryUsageCommitted",
		}),
		heapMemoryUsageInit: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageInit",
			Help:      "heapMemoryUsageInit",
		}),
		heapMemoryUsageMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageMax",
			Help:      "heapMemoryUsageMax",
		}),
		heapMemoryUsageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heapMemoryUsageUsed",
			Help:      "heapMemoryUsageUsed",
		}),

		VolumeFailuresTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "VolumeFailuresTotal",
			Help:      "VolumeFailuresTotal",
		}),
		EstimatedCapacityLostTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "EstimatedCapacityLostTotal",
			Help:      "EstimatedCapacityLostTotal",
		}),
		TotalFileOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "TotalFileOps",
			Help:      "TotalFileOps",
		}),
		GetBlockLocations: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GetBlockLocations",
			Help:      "GetBlockLocations",
		}),
		FilesCreated: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "FilesCreated",
			Help:      "FilesCreated",
		}),
		CreateFileOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CreateFileOps",
			Help:      "CreateFileOps",
		}),
		CacheReportNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CacheReportNumOps",
			Help:      "CacheReportNumOps",
		}),
		CacheReportAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "CacheReportAvgTime",
			Help:      "CacheReportAvgTime",
		}),
		BlockReportNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BlockReportNumOps",
			Help:      "BlockReportNumOps",
		}),
		BlockReportAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "BlockReportAvgTime",
			Help:      "BlockReportAvgTime",
		}),
		AddBlockOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "AddBlockOps",
			Help:      "AddBlockOps",
		}),
		GcTimeMillis: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcTimeMillis",
			Help:      "GcTimeMillis",
		}),
		GcTimeMillisParNew: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcTimeMillisParNew",
			Help:      "GcTimeMillisParNew",
		}),
		GcTimeMillisConcurrentMarkSweep: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcTimeMillisConcurrentMarkSweep",
			Help:      "GcTimeMillisConcurrentMarkSweep",
		}),
		GcCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcCount",
			Help:      "GcCount",
		}),
		GcCountParNew: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcCountParNew",
			Help:      "GcCountParNew",
		}),

		GcCountConcurrentMarkSweep: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GcCountConcurrentMarkSweep",
			Help:      "GcCountConcurrentMarkSweep",
		}),
		ThreadsBlocked: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "ThreadsBlocked",
			Help:      "ThreadsBlocked",
		}),
		GetListingAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GetListingAvgTime",
			Help:      "GetListingAvgTime",
		}),
		GetFileInfoAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "GetFileInfoAvgTime",
			Help:      "GetFileInfoAvgTime",
		}),
	}
}

// Describe implements the prometheus.Collector interface.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.MissingBlocks.Describe(ch)
	e.CapacityTotal.Describe(ch)
	e.CapacityUsed.Describe(ch)
	e.CapacityRemaining.Describe(ch)
	e.CapacityUsedNonDFS.Describe(ch)
	e.BlocksTotal.Describe(ch)
	e.FilesTotal.Describe(ch)
	e.CorruptBlocks.Describe(ch)
	e.ExcessBlocks.Describe(ch)
	e.StaleDataNodes.Describe(ch)
	e.TotalLoad.Describe(ch)
	e.ScheduledReplicationBlocks.Describe(ch)
	e.PendingReplicationBlocks.Describe(ch)
	e.pnGcCount.Describe(ch)
	e.pnGcTime.Describe(ch)
	e.cmsGcCount.Describe(ch)
	e.cmsGcTime.Describe(ch)
	e.heapMemoryUsageCommitted.Describe(ch)
	e.heapMemoryUsageInit.Describe(ch)
	e.heapMemoryUsageMax.Describe(ch)
	e.heapMemoryUsageUsed.Describe(ch)
	e.VolumeFailuresTotal.Describe(ch)
	e.EstimatedCapacityLostTotal.Describe(ch)
	e.TotalFileOps.Describe(ch)
	e.GetBlockLocations.Describe(ch)
	e.FilesCreated.Describe(ch)
	e.CreateFileOps.Describe(ch)
	e.CacheReportNumOps.Describe(ch)
	e.CacheReportAvgTime.Describe(ch)
	e.BlockReportNumOps.Describe(ch)
	e.BlockReportAvgTime.Describe(ch)
	e.AddBlockOps.Describe(ch)
	e.GcTimeMillis.Describe(ch)
	e.GcTimeMillisParNew.Describe(ch)
	e.GcTimeMillisConcurrentMarkSweep.Describe(ch)
	e.GcCount.Describe(ch)
	e.GcCountParNew.Describe(ch)
	e.GcCountConcurrentMarkSweep.Describe(ch)
	e.ThreadsBlocked.Describe(ch)
	e.GetListingAvgTime.Describe(ch)
	e.GetFileInfoAvgTime.Describe(ch)
}

// Collect implements the prometheus.Collector interface.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	resp, err := http.Get(e.url)
	if err != nil {
		//log.Error(err)
		fmt.Errorf("erro:", err)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//log.Error(err)
		fmt.Errorf("erro:", err)
	}
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil {
		//log.Error(err)
		fmt.Errorf("erro:", err)
	}
	// {"beans":[{"name":"Hadoop:service=NameNode,name=FSNamesystem", ...}, {"name":"java.lang:type=MemoryPool,name=Code Cache", ...}, ...]}
	m := f.(map[string]interface{})
	// [{"name":"Hadoop:service=NameNode,name=FSNamesystem", ...}, {"name":"java.lang:type=MemoryPool,name=Code Cache", ...}, ...]
	var nameList = m["beans"].([]interface{})
	for _, nameData := range nameList {
		nameDataMap := nameData.(map[string]interface{})

		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystem" {
			e.MissingBlocks.Set(nameDataMap["MissingBlocks"].(float64))
			e.CapacityTotal.Set(nameDataMap["CapacityTotal"].(float64))
			e.CapacityUsed.Set(nameDataMap["CapacityUsed"].(float64))
			e.CapacityRemaining.Set(nameDataMap["CapacityRemaining"].(float64))
			e.CapacityUsedNonDFS.Set(nameDataMap["CapacityUsedNonDFS"].(float64))
			e.BlocksTotal.Set(nameDataMap["BlocksTotal"].(float64))
			e.FilesTotal.Set(nameDataMap["FilesTotal"].(float64))
			e.CorruptBlocks.Set(nameDataMap["CorruptBlocks"].(float64))
			e.ExcessBlocks.Set(nameDataMap["ExcessBlocks"].(float64))
			e.StaleDataNodes.Set(nameDataMap["StaleDataNodes"].(float64))
			e.TotalLoad.Set(nameDataMap["TotalLoad"].(float64))
			e.ScheduledReplicationBlocks.Set(nameDataMap["ScheduledReplicationBlocks"].(float64))
			e.PendingReplicationBlocks.Set(nameDataMap["PendingReplicationBlocks"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystemState" {
			e.VolumeFailuresTotal.Set(nameDataMap["VolumeFailuresTotal"].(float64))
			e.EstimatedCapacityLostTotal.Set(nameDataMap["EstimatedCapacityLostTotal"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=NameNodeActivity" {
			e.TotalFileOps.Set(nameDataMap["TotalFileOps"].(float64))
			e.GetBlockLocations.Set(nameDataMap["GetBlockLocations"].(float64))
			e.FilesCreated.Set(nameDataMap["FilesCreated"].(float64))
			e.CreateFileOps.Set(nameDataMap["CreateFileOps"].(float64))
			e.CacheReportNumOps.Set(nameDataMap["CacheReportNumOps"].(float64))
			e.CacheReportAvgTime.Set(nameDataMap["CacheReportAvgTime"].(float64))
			e.BlockReportNumOps.Set(nameDataMap["BlockReportNumOps"].(float64))
			e.BlockReportAvgTime.Set(nameDataMap["BlockReportAvgTime"].(float64))
			e.AddBlockOps.Set(nameDataMap["AddBlockOps"].(float64))

		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=JvmMetrics" {
			e.GcTimeMillis.Set(nameDataMap["GcTimeMillis"].(float64))
			e.GcTimeMillisParNew.Set(nameDataMap["GcTimeMillisParNew"].(float64))
			e.GcTimeMillisConcurrentMarkSweep.Set(nameDataMap["GcTimeMillisConcurrentMarkSweep"].(float64))
			e.GcCount.Set(nameDataMap["GcCount"].(float64))
			e.GcCountParNew.Set(nameDataMap["GcCountParNew"].(float64))
			e.GcCountConcurrentMarkSweep.Set(nameDataMap["GcCountConcurrentMarkSweep"].(float64))
			e.ThreadsBlocked.Set(nameDataMap["ThreadsBlocked"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=RpcDetailedActivityForPort8020" {
			e.GetListingAvgTime.Set(nameDataMap["GetListingAvgTime"].(float64))
			e.GetFileInfoAvgTime.Set(nameDataMap["GetFileInfoAvgTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=GarbageCollector,name=ParNew" {
			//e.pnGcCount.Set(nameDataMap["CollectionCount"].(float64))
			e.pnGcCount.Add(nameDataMap["CollectionCount"].(float64))
			//e.pnGcTime.Set(nameDataMap["CollectionTime"].(float64))
			e.pnGcTime.Add(nameDataMap["CollectionTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=GarbageCollector,name=ConcurrentMarkSweep" {
			//e.cmsGcCount.Set(nameDataMap["CollectionCount"].(float64))
			//e.cmsGcTime.Set(nameDataMap["CollectionTime"].(float64))
			e.cmsGcCount.Add(nameDataMap["CollectionCount"].(float64))
			e.cmsGcTime.Add(nameDataMap["CollectionTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Memory" {
			heapMemoryUsage := nameDataMap["HeapMemoryUsage"].(map[string]interface{})
			e.heapMemoryUsageCommitted.Set(heapMemoryUsage["committed"].(float64))
			e.heapMemoryUsageInit.Set(heapMemoryUsage["init"].(float64))
			e.heapMemoryUsageMax.Set(heapMemoryUsage["max"].(float64))
			e.heapMemoryUsageUsed.Set(heapMemoryUsage["used"].(float64))
		}

	}
	e.MissingBlocks.Collect(ch)
	e.CapacityTotal.Collect(ch)
	e.CapacityUsed.Collect(ch)
	e.CapacityRemaining.Collect(ch)
	e.CapacityUsedNonDFS.Collect(ch)
	e.BlocksTotal.Collect(ch)
	e.FilesTotal.Collect(ch)
	e.CorruptBlocks.Collect(ch)
	e.ExcessBlocks.Collect(ch)
	e.StaleDataNodes.Collect(ch)
	e.TotalLoad.Collect(ch)
	e.ScheduledReplicationBlocks.Collect(ch)
	e.PendingReplicationBlocks.Collect(ch)
	e.pnGcCount.Collect(ch)
	e.pnGcTime.Collect(ch)
	e.cmsGcCount.Collect(ch)
	e.cmsGcTime.Collect(ch)
	e.heapMemoryUsageCommitted.Collect(ch)
	e.heapMemoryUsageInit.Collect(ch)
	e.heapMemoryUsageMax.Collect(ch)
	e.heapMemoryUsageUsed.Collect(ch)

	e.VolumeFailuresTotal.Collect(ch)
	e.EstimatedCapacityLostTotal.Collect(ch)
	e.TotalFileOps.Collect(ch)
	e.GetBlockLocations.Collect(ch)
	e.FilesCreated.Collect(ch)
	e.CreateFileOps.Collect(ch)
	e.CacheReportNumOps.Collect(ch)
	e.CacheReportAvgTime.Collect(ch)
	e.BlockReportNumOps.Collect(ch)
	e.BlockReportAvgTime.Collect(ch)
	e.AddBlockOps.Collect(ch)
	e.GcTimeMillis.Collect(ch)
	e.GcTimeMillisParNew.Collect(ch)
	e.GcTimeMillisConcurrentMarkSweep.Collect(ch)
	e.GcCount.Collect(ch)
	e.GcCountParNew.Collect(ch)
	e.GcCountConcurrentMarkSweep.Collect(ch)
	e.ThreadsBlocked.Collect(ch)
	e.GetListingAvgTime.Collect(ch)
	e.GetFileInfoAvgTime.Collect(ch)
}

func main() {
	flag.Parse()

	exporter := NewExporter(*namenodeJmxUrl)
	prometheus.MustRegister(exporter)

	//log.Printf("Starting Server: %s", *listenAddress)
	fmt.Printf("Starting Server: %s", *listenAddress)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>NameNode Exporter</title></head>
		<body>
		<h1>NameNode Exporter</h1>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>`))
	})
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		//log.Fatal(err)
		fmt.Errorf("erro:", err)
	}
}

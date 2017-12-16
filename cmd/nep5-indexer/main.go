package main

import (
	"flag"

	"github.com/dynamicgo/aliyunlog"
	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/inwecrypto/nep5-indexer"
	_ "github.com/lib/pq"
)

var logger = slf4go.Get("nep5-indexer")
var configpath = flag.String("conf", "./nep5.json", "neo indexer config file path")

func main() {

	flag.Parse()

	neocnf, err := config.NewFromFile(*configpath)

	if err != nil {
		logger.ErrorF("load neo config err , %s", err)
		return
	}

	factory, err := aliyunlog.NewAliyunBackend(neocnf)

	if err != nil {
		logger.ErrorF("create aliyun log backend err , %s", err)
		return
	}

	slf4go.Backend(factory)

	monitor, err := indexer.NewMonitor(neocnf)

	if err != nil {
		logger.ErrorF("create neo monitor err , %s", err)
		return
	}

	monitor.Run()

}

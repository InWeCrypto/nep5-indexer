package indexer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/inwecrypto/neogo"
	"github.com/syndtr/goleveldb/leveldb"
)

var key = []byte("key")

// Monitor .
type Monitor struct {
	slf4go.Logger
	client       *neogo.Client
	pullDuration time.Duration
	db           *leveldb.DB
	etl          *ETL
	filepath     string
}

// Notify nep5 notify
type Notify struct {
	Tx    string `json:"txid"`
	Asset string `json:"contract"`
	State State  `json:"state"`
}

// State .
type State struct {
	Type  string   `json:"type"`
	Value []*Value `json:"value"`
}

// Value .
type Value struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// NewMonitor .
func NewMonitor(conf *config.Config) (*Monitor, error) {
	client := neogo.NewClient(conf.GetString("indexer.neo", "http://localhost:8545"))

	etl, err := newETL(conf)

	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(conf.GetString("indexer.localdb", "./cursor"), nil)

	if err != nil {
		return nil, err
	}

	return &Monitor{
		Logger:       slf4go.Get("neo-monitor"),
		client:       client,
		etl:          etl,
		pullDuration: time.Second * conf.GetDuration("indexer.pull", 2),
		db:           db,
		filepath:     conf.GetString("indexer.notification", "./notifications"),
	}, nil
}

// Run .
func (monitor *Monitor) Run() {
	ticker := time.NewTicker(monitor.pullDuration)

	for _ = range ticker.C {
		monitor.DebugF("fetch geth last block number ...")
		blocks, err := monitor.client.GetBlockCount()
		monitor.DebugF("fetch geth last block number -- success, %d", blocks)
		if err != nil {
			monitor.ErrorF("fetch geth blocks err, %s", err)
		}

		i := 0

		for monitor.getCursor()+1 < uint64(blocks) {
			i++
			if err := monitor.fetchNotify(); err != nil {
				monitor.ErrorF("fetch notify err %s", err)
				break
			}

			if i >= 2000 {
				break
			}
		}
	}
}

func (monitor *Monitor) fetchNotify() error {

	cursor := monitor.getCursor()

	block, err := monitor.client.GetBlockByIndex(int64(cursor))

	if err != nil {
		monitor.ErrorF("fetch geth block(%d) err, %s", cursor, err)
		return err
	}

	fileName := filepath.Join(monitor.filepath, fmt.Sprintf("block-%d.json", cursor))

	monitor.DebugF("load notify file %s", fileName)

	_, err = os.Stat(fileName)

	if err != nil {
		if os.IsNotExist(err) {
			monitor.setCursor(cursor + 1)
			monitor.DebugF("load notify file %s -- not found", fileName)
			return nil
		}

		return err
	}

	monitor.DebugF("load notify file %s -- found", fileName)

	data, err := ioutil.ReadFile(fileName)

	if err != nil {
		return err
	}

	monitor.DebugF("handle notify: %s", string(data))

	var notifies []*Notify

	if err = json.Unmarshal(data, &notifies); err != nil {
		return err
	}

	if err := monitor.etl.Handle(notifies, block); err != nil {
		return err
	}

	monitor.DebugF("handle notify -- success")

	monitor.setCursor(cursor + 1)

	return nil
}

func (monitor *Monitor) getCursor() uint64 {
	buff, err := monitor.db.Get(key, nil)

	if err != nil {
		monitor.ErrorF("get Monitor local cursor error :%s", err)
		return 0
	}

	if buff == nil {
		monitor.ErrorF("get Monitor local cursor error : cursor not exists")
		return 0
	}

	return binary.BigEndian.Uint64(buff)
}

func (monitor *Monitor) setCursor(cursor uint64) error {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, cursor)

	return monitor.db.Put(key, buff, nil)
}

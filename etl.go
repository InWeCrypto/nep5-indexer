package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/go-xorm/xorm"
	"github.com/inwecrypto/gomq"
	gomqkafka "github.com/inwecrypto/gomq-kafka"
	"github.com/inwecrypto/neodb"
	"github.com/inwecrypto/neogo"
)

// ETL .
type ETL struct {
	slf4go.Logger
	conf   *config.Config
	engine *xorm.Engine
	mq     gomq.Producer // mq producer
	topic  string
	client *neogo.Client
}

func newETL(conf *config.Config) (*ETL, error) {
	username := conf.GetString("indexer.neodb.username", "xxx")
	password := conf.GetString("indexer.neodb.password", "xxx")
	port := conf.GetString("indexer.neodb.port", "6543")
	host := conf.GetString("indexer.neodb.host", "localhost")
	scheme := conf.GetString("indexer.neodb.schema", "postgres")

	engine, err := xorm.NewEngine(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v host=%v dbname=%v port=%v sslmode=disable",
			username, password, host, scheme, port,
		),
	)

	if err != nil {
		return nil, err
	}

	mq, err := gomqkafka.NewAliyunProducer(conf)

	if err != nil {
		return nil, err
	}

	return &ETL{
		Logger: slf4go.Get("nep5-indexer-etl"),
		conf:   conf,
		engine: engine,
		mq:     mq,
		topic:  conf.GetString("aliyun.kafka.topic", "xxxxx"),
		client: neogo.NewClient(conf.GetString("indexer.neo", "http://localhost:8545")),
	}, nil
}
func b58encode(b []byte) (s string) {
	/* See https://en.bitcoin.it/wiki/Base58Check_encoding */

	const BitcoinBase58Table = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

	/* Convert big endian bytes to big int */
	x := new(big.Int).SetBytes(b)

	/* Initialize */
	r := new(big.Int)
	m := big.NewInt(58)
	zero := big.NewInt(0)
	s = ""

	/* Convert big int to string */
	for x.Cmp(zero) > 0 {
		/* x, r = (x / 58, x % 58) */
		x.QuoRem(x, m, r)
		/* Prepend ASCII character */
		s = string(BitcoinBase58Table[r.Int64()]) + s
	}

	return s
}

func b58checkencodeNEO(ver uint8, b []byte) (s string) {
	/* Prepend version */
	bcpy := append([]byte{ver}, b...)

	/* Create a new SHA256 context */
	sha256h := sha256.New()

	/* SHA256 Hash #1 */
	sha256h.Reset()
	sha256h.Write(bcpy)
	hash1 := sha256h.Sum(nil)

	/* SHA256 Hash #2 */
	sha256h.Reset()
	sha256h.Write(hash1)
	hash2 := sha256h.Sum(nil)

	/* Append first four bytes of hash */
	bcpy = append(bcpy, hash2[0:4]...)

	/* Encode base58 string */
	s = b58encode(bcpy)

	// /* For number of leading 0's in bytes, prepend 1 */
	// for _, v := range bcpy {
	// 	if v != 0 {
	// 		break
	// 	}
	// 	s = "1" + s
	// }

	return s
}

// Handle .
func (etl *ETL) Handle(notifies []*Notify, block *neogo.Block) error {

	utxos := make([]*neodb.Tx, 0)

	for _, notify := range notifies {
		if len(notify.State.Value) == 0 {
			continue
		}

		if notify.State.Value[0].Value != "7472616e73666572" {
			etl.DebugF("notify type is %s, skip it", notify.State.Value[0].Value)
			continue
		}

		fromBytes, err := hex.DecodeString(notify.State.Value[1].Value)

		if err != nil {
			etl.ErrorF("decode from %s err, %s", notify.State.Value[1].Value, err)
			continue
		}

		from := ""

		if len(fromBytes) > 0 {
			from = b58checkencodeNEO(0x17, fromBytes)
		}

		toBytes, err := hex.DecodeString(notify.State.Value[2].Value)

		if err != nil {
			etl.ErrorF("decode to %s err, %s", notify.State.Value[2].Value, err)
			continue
		}

		to := ""

		if len(toBytes) > 0 {
			to = b58checkencodeNEO(0x17, toBytes)
		}

		valueBytes, err := hex.DecodeString(notify.State.Value[3].Value)

		if err != nil {
			etl.ErrorF("decode value %s err, %s", notify.State.Value[3].Value, err)
			continue
		}

		utxos = append(utxos, &neodb.Tx{
			TX:         notify.Tx,
			Block:      uint64(block.Index),
			From:       from,
			To:         to,
			Asset:      notify.Asset,
			Value:      big.NewInt(0).SetBytes(valueBytes).Text(10),
			CreateTime: time.Unix(block.Time, 0),
		})
	}

	if len(utxos) > 0 {
		if err := etl.batchInsertTx(utxos); err != nil {
			return err
		}

		for _, utxo := range utxos {
			etl.DebugF("create tx %s from %s to %s", utxo.TX, utxo.From, utxo.To)
		}
	}

	return nil
}

func (etl *ETL) batchInsertTx(rows []*neodb.Tx) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&rows)

	return
}

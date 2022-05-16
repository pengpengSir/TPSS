module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/parser v0.0.0-20181122101858-d773202d5b1f
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537
	github.com/cznic/y v0.0.0-20181122101901-b05e8c2e8d7b
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/goccy/go-graphviz v0.0.9 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.6.2
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ofabry/go-callvis v0.6.1 // indirect
	github.com/pingcap-incubator/tinykv v0.0.0-20200320061650-f660f803e910
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200210140405-f8f9fb234798
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/prometheus/common v0.4.1 // indirect
	github.com/sirupsen/logrus v1.2.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spaolacci/murmur3 v1.1.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/atomic v1.6.0
	go.uber.org/automaxprocs v1.2.0
	go.uber.org/zap v1.14.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/image v0.0.0-20220413100746-70e8d0d3baa9 // indirect
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
	golang.org/x/sys v0.0.0-20220503163025-988cb79eb6c6 // indirect
	golang.org/x/text v0.3.7
	golang.org/x/tools v0.1.10
	golang.org/x/xerrors v0.0.0-20220411194840-2f41105eb62f // indirect
	google.golang.org/grpc v1.25.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

go 1.13

replace github.com/pingcap/check => github.com/tiancaiamao/check v0.0.0-20191119042138-8e73d07b629d

replace github.com/pingcap-incubator/tinykv v0.0.0-20200320061650-f660f803e910 => ../tinykv

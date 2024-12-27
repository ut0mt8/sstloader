package cassandra

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"sstloader/pkg/sstable"

	"github.com/gocql/gocql"
)

type CassandraLoader struct {
	Compress bool
	Debug    bool
	Seeds    string
	KS       string
	Table    string
	Timeout  int
	Retries  int
	Conns    int
	DC       string
	Username string
	Password string
	Errors   atomic.Uint64

	request string
	session *gocql.Session
}

func New() *CassandraLoader {
	return &CassandraLoader{}
}

func (cl *CassandraLoader) Prepare(sst *sstable.SSTable) error {
	var (
		partition      string
		clustering     string
		cname          string
		kind           string
		regularColumns string
		columnsFill    string
		pkNumber       int
	)

	// cassandra init
	cluster := gocql.NewCluster(cl.Seeds)
	cluster.Keyspace = cl.KS
	cluster.Consistency = gocql.Any // we don't want to wait
	cluster.ProtoVersion = 4        // null handling
	cluster.Timeout = time.Duration(cl.Timeout) * time.Millisecond
	cluster.WriteTimeout = time.Duration(cl.Timeout) * time.Millisecond
	cluster.NumConns = cl.Conns // theoricitally handled by the scylla driver
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cl.Retries}
	if cl.DC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cl.DC))
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}
	if cl.Compress {
		cluster.Compressor = &gocql.SnappyCompressor{} // only compressor supported
	}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cl.Username,
		Password: cl.Password,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	// session is goroutine safe
	cl.session = session

	// construct insert query

	// get partition and clustering key
	// TODO only text supported
	req := "SELECT column_name, kind FROM system_schema.columns where keyspace_name = '%s' and table_name = '%s'"
	iter := cl.session.Query(fmt.Sprintf(req, cl.KS, cl.Table)).Consistency(gocql.LocalQuorum).Iter()

	for iter.Scan(&cname, &kind) {
		if kind == "partition_key" {
			partition = partition + cname + ","
			columnsFill = columnsFill + "?,"
			pkNumber++
		} else if kind == "clustering" {
			clustering = clustering + cname + ","
			columnsFill = columnsFill + "?,"
		}
	}

	// FIXME find better to pass it to partitionReader
	if pkNumber > 1 {
		sst.Compound = true
	}

	// get columns from schemas (sst side)
	for i := 0; i < len(sstable.Schema); i++ {
		regularColumns = regularColumns + sstable.Schema[i].Name + ","
		columnsFill = columnsFill + "?,"
	}

	regularColumns = strings.Trim(regularColumns, ",")
	columnsFill = strings.Trim(columnsFill, ",")

	// insert reqyest
	cl.request = "INSERT INTO " + cl.KS + "." + cl.Table +
		" (" + partition + clustering + regularColumns + ") VALUES (" + columnsFill + ")"
	if cl.Debug {
		fmt.Printf("(debug) query: %s \n", cl.request)
	}

	return nil
}

func (cl *CassandraLoader) Load(v []any) {
	err := cl.session.Query(cl.request).Bind(v...).Exec()
	if err != nil {
		cl.Errors.Add(1)
		if cl.Debug {
			fmt.Printf("(debug) query error: %v\n", err)
		}
	}
}

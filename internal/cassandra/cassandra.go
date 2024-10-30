package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"strings"
	"sync/atomic"
	"time"

	"sstloader/pkg/sstable"
)

type CassandraLoader struct {
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

	query *gocql.Query
}

func New() *CassandraLoader {
	return &CassandraLoader{}
}

func (cl *CassandraLoader) Prepare(sst *sstable.SSTable) error {

	var (
		partition     string
		clustering    string
		cname         string
		kind          string
		regularColums string
		columsFill    string
		pkNumber      int
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
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cl.DC))

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cl.Username,
		Password: cl.Password,
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session: %v\n", err)
	}

	// construct insert query

	// get partition and clustering key
	// TODO only text supported
	req := "SELECT column_name, kind FROM system_schema.columns where keyspace_name = '%s' and table_name = '%s'"
	iter := session.Query(fmt.Sprintf(req, cl.KS, cl.Table)).Consistency(gocql.LocalQuorum).Iter()

	for iter.Scan(&cname, &kind) {
		if kind == "partition_key" {
			partition = partition + cname + ","
			columsFill = columsFill + "?,"
			pkNumber++
		} else if kind == "clustering" {
			clustering = clustering + cname + ","
			columsFill = columsFill + "?,"
		}
	}

	// FIXME find better to pass it to partitionReader
	if pkNumber > 1 {
		sst.Compound = true
	}

	// get columns from schemas (sst side)
	for i := 0; i < len(sstable.Schema); i++ {
		regularColums = regularColums + sstable.Schema[i].Name + ","
		columsFill = columsFill + "?,"
	}

	regularColums = strings.Trim(regularColums, ",")
	columsFill = strings.Trim(columsFill, ",")

	// insert reqyest
	req = "INSERT INTO " + cl.KS + "." + cl.Table + " (" + partition + clustering + regularColums + ") VALUES (" + columsFill + ") "
	if cl.Debug {
		fmt.Printf("(debug) query: %s \n", req)
	}

	cl.query = session.Query(req)

	return nil
}

func (cl *CassandraLoader) Load(v []any) {
	err := cl.query.Bind(v...).Exec()
	if err != nil {
		cl.Errors.Add(1)
		if cl.Debug {
			fmt.Printf("(debug) query error: %v\n", err)
		}
	}
}

package tracedsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/lib/pq"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MaxRdsRetriesSec = 5

type PgConnectorWithRds struct {
	config aws.Config
	isRds  bool

	rdsDb, postgresDbName string
	host                  string
	port                  int32
	user, secretName      string

	sslMode   string
	sslCaPath string

	mtx        sync.Mutex
	connString string
	delegate   driver.Connector
}

// Create a Postgres connector to use with NewRelic. The PgConnector supports
// resolving RDS endpoints and AWS secrets-based authentication.
// Example conn string: "rdsDb=terra-rds dbName=terra secretName=terra-rds-admin"
func MakePgConnector(ctx context.Context, connStr string, sslCaPath string,
	config aws.Config) (*PgConnectorWithRds, error) {

	// Not an RDS-format connection string?
	if strings.HasPrefix(connStr, "postgres://") {
		connector, err := pq.NewConnector(connStr)
		if err != nil {
			return nil, err
		}

		res := &PgConnectorWithRds{
			isRds:      false,
			connString: connStr,
			delegate:   connector,
		}
		return res, nil
	}

	// Split the string into key-value pairs. TODO: escaping for spaces?
	params := map[string]string{}
	for _, s := range strings.Split(connStr, " ") {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad RDS connection string: %s", connStr)
		}
		params[parts[0]] = parts[1]
	}

	// Extract params
	rdsDb := params["rdsDb"]
	if rdsDb == "" {
		return nil, fmt.Errorf("bad RDS connection string: %s (no rdsDb value)", connStr)
	}
	user := params["user"]
	if user == "" {
		user = "postgres"
	}
	secretName := params["secretName"]
	if secretName == "" {
		secretName = fmt.Sprintf("db/%s", rdsDb)
	}
	dbName := params["dbName"]
	if dbName == "" {
		dbName = rdsDb
	}
	sslMode := params["sslMode"]

	port, _ := strconv.ParseInt(params["port"], 10, 32)
	if port == 0 {
		port = 5432
	}

	// Resolve host?
	host := params["host"]
	if host == "" {
		var err error
		host, port, err = resolveHost(ctx, config, rdsDb)
		if err != nil {
			return nil, err
		}
	}

	res := &PgConnectorWithRds{
		isRds:            true,
		config:           config,
		connString:       connStr,
		rdsDb:            rdsDb,
		postgresDbName:   dbName,
		sslMode:          sslMode,
		sslCaPath:        sslCaPath,
		user:             user,
		secretName:       secretName,
		host:             host,
		port:             int32(port),
	}

	err := res.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func resolveHost(ctx context.Context, config aws.Config, db string) (string, int64, error) {
	cli := rds.New(config)
	clusters, err := cli.DescribeDBClustersRequest(&rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(db),
		IncludeShared:       aws.Bool(true),
	}).Send(ctx)
	if err != nil {
		return "", 0, err
	}
	if len(clusters.DBClusters) != 1 {
		return "", 0, fmt.Errorf("can't find the unique cluster %s", db)
	}

	cluster := clusters.DBClusters[0]
	if cluster.Endpoint == nil {
		return "", 0, fmt.Errorf("cluster %s has no endpoint", db)
	}
	return *cluster.Endpoint, *cluster.Port, nil
}

func (pc *PgConnectorWithRds) getCurrentPassword(ctx context.Context) (string, error) {
	sm := secretsmanager.New(pc.config)

	//Create a Secrets Manager client
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(pc.secretName),
		// VersionStage defaults to AWSCURRENT if unspecified
		VersionStage: aws.String("AWSCURRENT"),
	}

	result, err := sm.GetSecretValueRequest(input).Send(ctx)
	if err != nil {
		return "", err
	}

	if aws.StringValue(result.SecretString) == "" {
		return "", fmt.Errorf("no string secret")
	}
	return *result.SecretString, nil
}

func (pc *PgConnectorWithRds) getConnString(pass string) string {
	// If the host is not autoresolved, we're likely using a proxy and can not
	// verify the host name.
	sslMode := ""
	if pc.sslMode != "" && pc.sslMode != "disabled" {
		sslMode = fmt.Sprintf("sslmode=verify-full sslrootcert=%s", pc.sslCaPath)
	}
	conn := fmt.Sprintf("host=%s port=%d database=%s user=%s %s password=%s",
		pc.host, pc.port, pc.postgresDbName, pc.user, sslMode, pass)
	return conn
}

func (pc *PgConnectorWithRds) Driver() driver.Driver {
	return pc.delegate.Driver()
}

func (pc *PgConnectorWithRds) tryConnection(ctx context.Context) (driver.Conn, error) {
	pc.mtx.Lock()
	defer pc.mtx.Unlock()

	if pc.delegate != nil {
		conn, err := pc.delegate.Connect(ctx)
		if err == nil {
			return conn, nil
		}
		pc.delegate = nil
	}

	pass, err := pc.getCurrentPassword(ctx)
	if err != nil {
		return nil, err
	}

	connector, err := pq.NewConnector(pc.getConnString(pass))
	if err != nil {
		return nil, err
	}

	conn, err := connector.Connect(ctx)
	if err == nil {
		pc.delegate = connector
		return conn, nil
	}

	return nil, err
}

func (pc *PgConnectorWithRds) Connect(ctx context.Context) (driver.Conn, error) {
	if !pc.isRds {
		return pc.delegate.Connect(ctx)
	}

	// A small retry loop to compensate for the possibility of secret rotation
	start := time.Now().Unix()
	for ; ; {
		conn, err := pc.tryConnection(ctx)
		if err == nil {
			return conn, err
		}

		if time.Now().Unix()-start > MaxRdsRetriesSec {
			return nil, err
		}

		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		default:
		}
	}
}

func (pc *PgConnectorWithRds) Ping(ctx context.Context) error {
	conn, err := pc.Connect(ctx)
	if err != nil {
		return err
	}
	//noinspection GoUnhandledErrorResult
	defer conn.Close()

	return conn.(driver.Pinger).Ping(ctx)
}

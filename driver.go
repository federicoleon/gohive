package gohive

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

const (
	HIVE_DRIVER_NAME = "hive"
)

var (
	ERROR_NOT_SUPPORTED            = fmt.Errorf("Hive: Not supported")
	ERROR_NOT_IMPLEMENTED          = fmt.Errorf("Hive: Not implemented")
	ERROR_PREPARED_QUERY_NOT_FOUND = fmt.Errorf("Hive: Prepared query not found")
	ERROR_INTERNAL                 = fmt.Errorf("Hive: Internal error")
)

func init() {
	sql.Register(HIVE_DRIVER_NAME, &hiveDrv{})
}

// Implements Driver interface:
type hiveDrv struct{}

func (drv *hiveDrv) Open(dsn string) (driver.Conn, error) {
	connection, err := OpenHiveConnection(dsn)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

// Implements driver.Conn interface:
type hiveConn struct {
	hiveVersion string
	host        string
	port        int
	user        string
	password    string
	dbName      string
	args        map[string]interface{}
}

func (conn *hiveConn) Begin() (driver.Tx, error) {
	//TODO: Implement!
	return nil, ERROR_NOT_SUPPORTED
}

func (conn *hiveConn) Close() error {
	//TODO: Implement!
	return nil
}

func (conn *hiveConn) Prepare(query string) (driver.Stmt, error) {
	statement := &hiveStatement{connection: conn}
	//TODO: Implement!
	return statement, nil
}

func (conn *hiveConn) performExecution(query string, requestValues []string) (driver.Result, error) {
	//TODO: Implement!
	return nil, nil
}

func (conn *hiveConn) performQuery(query string, requestValues []string) (driver.Rows, error) {
	//TODO: Implement!
	return nil, nil
}

// Implements driver.Stmt interface:
type hiveStatement struct {
	connection    *hiveConn
	preparedQuery string
	argsCount     int
}

func (stmt *hiveStatement) Close() error {
	stmt.connection = nil
	stmt = nil
	return nil
}

func (stmt *hiveStatement) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.preparedQuery == "" {
		return nil, ERROR_PREPARED_QUERY_NOT_FOUND
	}
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	return stmt.connection.performExecution("", requestValues)
}

func (stmt *hiveStatement) prepareRequest(args []driver.Value) ([]string, error) {
	//TODO: Implement!
	return nil, nil
}

func (stmt *hiveStatement) NumInput() int {
	return stmt.argsCount
}

func (stmt *hiveStatement) Query(args []driver.Value) (driver.Rows, error) {
	if stmt.preparedQuery == "" {
		return nil, ERROR_PREPARED_QUERY_NOT_FOUND
	}
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.connection.performQuery("", requestValues)
	return rows, err
}

func OpenHiveConnection(dsn string) (driver.Conn, error) {
	config, errConf := ParseDSN(dsn)
	if errConf != nil {
		return nil, errConf
	}
	connection := &hiveConn{hiveVersion: config.HiveVersion, host: config.Host,
		port: config.Port, user: config.User, password: config.Password,
		dbName: config.DBName, args: config.Args}

	return connection, nil
}

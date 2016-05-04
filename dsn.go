package gohive

import (
	"errors"
	"fmt"
	"strings"
)

type Config struct {
	HiveVersion string // Hive version. Supporting hive and hive2
	Host        string // Connection host
	Port        int    // Connection port
	User        string // Username
	Password    string // Password (requires User)
	DBName      string // Database name
}

const (
	HIVE   = "hive"
	HIVE_2 = "hive2"
)

func ParseDSN(dsn string) (*Config, error) {
	config := new(Config)

	var err error

	var hiveVersion string
	hiveVersion, err = getHiveVersion(dsn)
	if err != nil {
		return nil, err
	}
	config.HiveVersion = hiveVersion

	// TODO: Parse host, port, user, password and dbname from dsn.
	config.Host = ""
	config.Port = 100
	config.User = ""
	config.Password = ""
	config.DBName = ""

	return config, nil
}

func getHiveVersion(dsn string) (string, error) {
	dsnValues := strings.Split(dsn, "://")
	fmt.Println(len(dsnValues))
	fmt.Println(dsnValues)
	var result string
	if len(dsnValues) > 1 {
		switch dsnValues[0] {
		case HIVE:
			result = HIVE
			break
		case HIVE_2:
			result = HIVE_2
			break
		}
	}

	if result == "" {
		return "", errors.New(fmt.Sprintf("Invalid Hive version. Expected %s or %s", HIVE, HIVE_2))
	}
	return result, nil
}

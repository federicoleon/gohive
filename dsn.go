package gohive

import (
	"errors"
	"strconv"
	"strings"
)

type Config struct {
	HiveVersion string                 // Hive version. Supporting hive and hive2
	Host        string                 // Connection host
	Port        int                    // Connection port
	User        string                 // Username
	Password    string                 // Password (requires User)
	DBName      string                 // Database name
	Args        map[string]interface{} // Extra parameters received in dsn
}

const (
	HIVE           = "hive"
	HIVE_2         = "hive2"
	PARAM_USER     = "user"
	PARAM_PASSWORD = "password"
)

var (
	ERROR_INVALID_CONNECTION_STRING = "Hive: Invalid connection string"
	ERROR_INVALID_PORT              = "Hive: Invalid port in connection string"
	ERROR_INVALID_DATABASE_NAME     = "Hive: Invalid database name"
)

func ParseDSN(dsn string) (*Config, error) {
	config := new(Config)
	err := getHiveConfigFromDSN(dsn, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func getHiveConfigFromDSN(dsn string, configP **Config) error {
	dsnValues := strings.Split(dsn, "://")
	if len(dsnValues) != 2 {
		return errors.New(ERROR_INVALID_CONNECTION_STRING)
	}
	processHiveVersion(dsnValues[0], configP)
	processHiveConnectionString(dsnValues[1], configP)
	return nil
}

func processHiveVersion(hiveP string, confP **Config) {
	config := *confP
	config.HiveVersion = HIVE_2
	switch hiveP {
	case HIVE:
		config.HiveVersion = HIVE
		break
	}
}

func processHiveConnectionString(connP string, config **Config) error {
	if config == nil {
		return errors.New("Couldn't process nil config")
	}

	connValues := strings.Split(connP, ";")
	err := processHostPortAndDB(connValues[0], config)
	if err != nil {
		return err
	}

	if len(connValues) > 1 {
		err = processExtraParameters(connValues[1:], config)
	}

	if err != nil {
		return err
	}
	return nil
}

func processHostPortAndDB(hostAndPort string, confP **Config) error {
	if confP == nil {
		return errors.New("Couldn't process nil config")
	}

	hostValues := strings.Split(hostAndPort, ":")
	if len(hostValues) != 2 {
		return errors.New(ERROR_INVALID_CONNECTION_STRING)
	}
	config := *confP

	config.Host = hostValues[0]

	portAndDB := strings.Split(hostValues[1], "/")
	if len(portAndDB) != 2 {
		return errors.New(ERROR_INVALID_DATABASE_NAME)
	}
	port, err := strconv.Atoi(portAndDB[0])
	if err != nil {
		return errors.New(ERROR_INVALID_PORT)
	}

	config.Port = port
	config.DBName = portAndDB[1]

	return nil
}

func processExtraParameters(params []string, confP **Config) error {
	if confP == nil {
		return errors.New("Couldn't process nil config")
	}
	config := *confP
	args := make(map[string]interface{})
	for _, value := range params {
		param := strings.Split(value, "=")
		if len(param) == 2 {
			switch param[0] {
			case PARAM_USER:
				config.User = param[1]
				break
			case PARAM_PASSWORD:
				config.Password = param[1]
				break
			default:
				args[param[0]] = param[1]
				break
			}
		}
	}
	config.Args = args
	return nil
}

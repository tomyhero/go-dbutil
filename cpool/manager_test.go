package cpool

import (
	"github.com/lestrrat/go-test-mysqld"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPing(t *testing.T) {

	mysqld, _ := mysqltest.NewMysqld(nil)
	defer mysqld.Stop()
	dsn := mysqld.Datasource("test", "", "", 0)

	config := DatabaseConfig{
		Master: DatabaseSetting{DSN: dsn},
		Slaves: []DatabaseSetting{{DSN: dsn}},
	}

	Setup(config)
	err := Instance().Ping()
	assert.Nil(t, err)
}

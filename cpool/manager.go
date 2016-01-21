package cpool

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
)

/*

 Database Connectionの実装を保持
 *sql.DB,*sql.Tx を受け取る用途

*/
type Conn interface {
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type DatabaseConfig struct {
	Master DatabaseSetting
	Slaves []DatabaseSetting
}

type DatabaseSetting struct {
	DSN string
}

var instance *Manager

// Manager is holding conns for slaves/master databases
type Manager struct {
	// hold slaves conns
	slaves []*sql.DB

	// hold master conn
	master *sql.DB
}

func Instance() *Manager {
	return instance
}

func Setup(config DatabaseConfig) {
	cm, err := NewManager(config)
	if err != nil {
		panic(err)
	}
	instance = cm
}

func NewManager(config DatabaseConfig) (*Manager, error) {
	manager := Manager{}
	slaveDSNs := []string{}

	for i := range config.Slaves {
		slaveDSNs = append(slaveDSNs, config.Slaves[i].DSN)
	}

	err := manager.Connect(config.Master.DSN, slaveDSNs)

	return &manager, err

}

// Master database conn
func (self *Manager) Master() *sql.DB {
	return self.master
}

// pick one of random Slave database conn
func (self *Manager) Slave() *sql.DB {
	index := rand.Intn(len(self.slaves))
	return self.slaves[index]
}

// Connect to databases
func (self *Manager) Connect(masterDSN string, slaveDNS []string) error {

	master, err := sql.Open("mysql", masterDSN)
	if err != nil {
		return err
	}
	self.master = master

	slaves := []*sql.DB{}
	for i := range slaveDNS {
		slave, err := sql.Open("mysql", slaveDNS[i])
		if err != nil {
			return err
		}
		slaves = append(slaves, slave)
	}
	self.slaves = slaves
	return nil
}

// Ping to databases
func (self *Manager) Ping() error {

	for _, db := range append(self.slaves, self.master) {
		err := db.Ping()
		if err != nil {
			return err
		}
	}
	return nil
}

package example

import (
	"github.com/lestrrat/go-test-mysqld"
	"github.com/tomyhero/go-dbutil/cpool"
	"testing"
)

func TestMember(t *testing.T) {

	mysqld, _ := mysqltest.NewMysqld(nil)
	defer mysqld.Stop()
	dsn := mysqld.Datasource("test", "", "", 0)

	config := cpool.DatabaseConfig{
		Master: cpool.DatabaseSetting{DSN: dsn},
		Slaves: []cpool.DatabaseSetting{{DSN: dsn}},
	}

	cpool.Setup(config)

	_, err := cpool.Instance().Master().Exec("create table member ( member_id int unsigned not null auto_increment, name text,PRIMARY KEY (member_id))")

	if err != nil {
		panic(err)
	}

	m := NewMember(cpool.Instance().Master())
	memberID := m.Insert("Taro")
	if _, has := m.Lookup(0); has == true {
		t.Fail()
	}

	if _, has := m.Lookup(memberID); has != true {
		t.Fail()
	}

	objs := m.Search()
	if objs[0].MemberID == 0 {
		t.Fail()
	}

	if objs[0].Name != "Taro" {
		t.Fail()
	}
}

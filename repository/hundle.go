package repository

import (
	"database/sql"
	sq "github.com/Masterminds/squirrel"
	log "github.com/Sirupsen/logrus"
	"github.com/tomyhero/go-dbutil/cpool"
	"reflect"
)

type Model interface {
	FieldHolder() []interface{}
	GetFields() string
	GetTable() string
	GetPrimaryKeys() []string
}

// Database Hundle ロジッククラス
type Hundle struct {
	// database conn
	Conn cpool.Conn
}

func NewHundle(conn cpool.Conn) Hundle {
	return Hundle{Conn: conn}
}

// insert処理、last inserted idを返却する
func (self *Hundle) InsertX(obj Model, values map[string]interface{}) int {

	//values["created_at"] = sq.Expr("NOW()")
	//values["updated_at"] = sq.Expr("NOW()")

	b := sq.Insert(obj.GetTable()).SetMap(values)

	s, args, err := b.ToSql()
	if err != nil {
		log.WithFields(log.Fields{
			"table":  obj.GetTable(),
			"values": values,
			"err":    err,
		}).Panic("Fail To Build Insert SQL")
	}

	res, err := self.Conn.Exec(s, args...)
	if err != nil {
		log.WithFields(log.Fields{
			"table":  obj.GetTable(),
			"values": values,
			"err":    err,
		}).Panic("Fail To Execute Insert SQL")
	}

	lastID, err := res.LastInsertId()
	if err != nil {
		log.WithFields(log.Fields{
			"table":  obj.GetTable(),
			"values": values,
			"err":    err,
		}).Panic("Fail To get Last Insert ID")
	}
	return int(lastID)
}

// PKからレコードを取得し、objに格納する
func (self *Hundle) LookupX(obj Model, ids ...interface{}) bool {
	b := sq.Select(obj.GetFields()).From(obj.GetTable())

	for i, field := range obj.GetPrimaryKeys() {
		b = b.Where(sq.Eq{field: ids[i]})
	}
	s, args, err := b.ToSql()

	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"ids":   ids,
			"err":   err,
		}).Panic("Fail To Build lookup SQL")
	}

	row := self.Conn.QueryRow(s, args...)
	err = self.RowScan(obj, row)

	if err == sql.ErrNoRows {
		return false
	} else if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"ids":   ids,
			"err":   err,
		}).Panic("Fail to lookup()")
	}

	return true
}

// lookup() + ロック
func (self *Hundle) LookupForUpdateX(obj Model, ids ...interface{}) bool {
	b := sq.Select(obj.GetFields()).From(obj.GetTable())

	for i, field := range obj.GetPrimaryKeys() {
		b = b.Where(sq.Eq{field: ids[i]})
	}
	b = b.Suffix("FOR UPDATE")
	s, args, err := b.ToSql()

	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"ids":   ids,
			"err":   err,
		}).Panic("Fail To Build lookupForUpdate SQL")
	}

	row := self.Conn.QueryRow(s, args...)
	err = self.RowScan(obj, row)

	if err == sql.ErrNoRows {
		return false
	} else if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"ids":   ids,
			"err":   err,
		}).Panic("Fail to lookupForUpdate()")
	}

	return true
}

// 1 recordのscan、格納
func (self *Hundle) RowScan(obj Model, row *sql.Row) error {
	err := row.Scan(obj.FieldHolder()...)

	if err == sql.ErrNoRows {
		return err
	} else if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Panic("Fail at rowScan()")
	}
	return nil
}

// 複数recordのscan、格納 rowsのClose処理も行う
func (self *Hundle) RowsScan(i interface{}, rows *sql.Rows) {
	iv := reflect.ValueOf(i).Elem()
	it := reflect.TypeOf(i).Elem().Elem().Elem()

	defer rows.Close()
	for rows.Next() {
		obj := reflect.New(it).Interface().(Model)
		rows.Scan(obj.FieldHolder()...)
		iv.Set(reflect.Append(iv, reflect.ValueOf(obj)))
	}
}
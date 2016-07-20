package repository

import (
	"database/sql"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	log "github.com/Sirupsen/logrus"
	"github.com/tomyhero/go-dbutil/cpool"
)

type Model interface {
	FieldHolder() []interface{}
	GetFields() string
	GetTable() string
	GetPrimaryKeys() []string
}

// Database Handle ロジッククラス
type Handle struct {
	// database conn
	Conn cpool.Conn
}

func NewHandle(conn cpool.Conn) Handle {
	return Handle{Conn: conn}
}

func (self *Handle) DeleteX(obj Model, buildFn func(sq.DeleteBuilder) sq.DeleteBuilder) int {

	b := sq.Delete(obj.GetTable())
	b = buildFn(b)
	s, args, err := b.ToSql()

	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"args":  args,
			"err":   err,
		}).Panic("Fail To Build DELETE SQL")
	}

	res, err := self.Conn.Exec(s, args...)
	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"args":  args,
			"err":   err,
		}).Panic("Fail To Execute DELTE SQL")
	}

	i, _ := res.RowsAffected()

	return int(i)
}

func (self *Handle) UpdateX(obj Model, values map[string]interface{}, buildFn func(sq.UpdateBuilder) sq.UpdateBuilder) int {

	b := sq.Update(obj.GetTable()).SetMap(values)
	b = buildFn(b)

	s, args, err := b.ToSql()
	if err != nil {
		log.WithFields(log.Fields{
			"table":  obj.GetTable(),
			"values": values,
			"err":    err,
		}).Panic("Fail To Build Update SQL")
	}

	res, err := self.Conn.Exec(s, args...)
	if err != nil {
		log.WithFields(log.Fields{
			"table":  obj.GetTable(),
			"values": values,
			"err":    err,
		}).Panic("Fail To Execute Update SQL")
	}

	i, _ := res.RowsAffected()

	return int(i)
}

// insert処理、last inserted idを返却する
func (self *Handle) InsertX(obj Model, values map[string]interface{}) int {

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

func (self *Handle) RetrieveX(obj Model, buildFn func(sq.SelectBuilder) sq.SelectBuilder) bool {
	b := sq.Select(obj.GetFields()).From(obj.GetTable())
	b = buildFn(b)
	s, args, err := b.ToSql()

	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"err":   err,
		}).Panic("Fail To Build RetrieveX SQL")
	}

	row := self.Conn.QueryRow(s, args...)
	err = self.RowScan(obj, row)

	if err == sql.ErrNoRows {
		return false
	} else if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"err":   err,
		}).Panic("Fail to RetrieveX()")
	}
	return true
}

// PKからレコードを取得し、objに格納する
func (self *Handle) LookupX(obj Model, ids ...interface{}) bool {
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
func (self *Handle) LookupForUpdateX(obj Model, ids ...interface{}) bool {
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

func (self *Handle) CountX(obj Model, buildFn func(sq.SelectBuilder) sq.SelectBuilder) int {
	b := sq.Select("count(*)").From(obj.GetTable())
	b = buildFn(b)
	s, args, err := b.ToSql()

	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"err":   err,
		}).Panic("Fail To Build CountX SQL")
	}

	count := 0
	err = self.Conn.QueryRow(s, args...).Scan(&count)

	if err == sql.ErrNoRows {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"err":   err,
		}).Panic("Fail To Get Count SQL")
	} else if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"err":   err,
		}).Panic("Fail to CountX()")
	}

	return count
}

func (self *Handle) SearchX(i interface{}, buildFn func(sq.SelectBuilder) sq.SelectBuilder) {
	obj := reflect.New(reflect.TypeOf(i).Elem().Elem().Elem()).Interface().(Model)
	//rows, err := self.Conn.Query("SELECT " + obj.GetFields() + " FROM " + obj.GetTable())
	b := sq.Select(obj.GetFields()).From(obj.GetTable())
	b = buildFn(b)
	rows, err := b.RunWith(self.Conn).Query()

	if err != nil {
		log.WithFields(log.Fields{
			"table": obj.GetTable(),
			"err":   err,
		}).Panic("Fail to SearchX()")
	}

	self.RowsScan(i, rows)
}

// 1 recordのscan、格納
func (self *Handle) RowScan(obj Model, row *sql.Row) error {
	return self.RowScanX(obj, row, func(obj Model, row *sql.Row) error {
		return row.Scan(obj.FieldHolder()...)
	})
}

// 複数recordのscan、格納 rowsのClose処理も行う
func (self *Handle) RowsScan(i interface{}, rows *sql.Rows) {
	self.RowsScanX(i, rows, func(obj Model, rows *sql.Rows) {
		rows.Scan(obj.FieldHolder()...)
	})
}

func (self *Handle) RowScanX(obj Model, row *sql.Row, buildFn func(Model, *sql.Row) error) error {
	err := buildFn(obj, row)
	if err == sql.ErrNoRows {
		return err
	} else if err != nil {
		log.WithFields(log.Fields{
			"err":   err,
			"table": obj.GetTable(),
		}).Panic("Fail at RowScanX()")
	}
	return nil
}

func (self *Handle) RowsScanX(i interface{}, rows *sql.Rows, buildFn func(Model, *sql.Rows)) {
	iv := reflect.ValueOf(i).Elem()
	it := reflect.TypeOf(i).Elem().Elem().Elem()
	defer rows.Close()
	for rows.Next() {
		obj := reflect.New(it).Interface().(Model)
		buildFn(obj, rows)
		iv.Set(reflect.Append(iv, reflect.ValueOf(obj)))
	}
}

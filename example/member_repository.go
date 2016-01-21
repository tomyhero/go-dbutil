package example

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/tomyhero/go-dbutil/cpool"
	"github.com/tomyhero/go-dbutil/repository"
)

func NewMember(conn cpool.Conn) *Member {
	return &Member{Handle: repository.NewHandle(conn)}
}

func (self *Member) GetTable() string {
	return "member"
}

func (self *Member) GetPrimaryKeys() []string {
	return []string{"member_id"}
}

func (self *Member) GetFields() string {
	return "member_id,name"
}

func (self *Member) FieldHolder() []interface{} {
	return []interface{}{
		&self.MemberID,
		&self.Name,
	}
}

func (self *Member) Lookup(memberID int) (*Member, bool) {
	obj := &Member{}
	res := self.LookupX(obj, memberID)
	return obj, res
}

func (self *Member) Insert(name string) int {
	obj := &Member{}
	return self.InsertX(obj, map[string]interface{}{"name": name})
}

func (self *Member) Search() []*Member {
	objs := []*Member{}
	self.SearchX(&objs, func(sb sq.SelectBuilder) sq.SelectBuilder {
		return sb
	})
	return objs
}

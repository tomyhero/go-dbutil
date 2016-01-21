package example

import (
	"github.com/tomyhero/go-dbutil/repository"
)

type Member struct {
	MemberID int
	Name     string
	repository.Handle
}

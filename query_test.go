package db

import (
	"testing"
)

func Test_Chain(t *testing.T) {
	Connect("127.0.0.1", "test", "test", "shopxo", "3306")

	ret, err := NewQuery().Db("shopxo").Table("spxo_admin").
		WhereOr("username=?").WhereOr("username=?").Value("admin").Value("test").Debug(true).List()

	t.Log(len(ret))
	t.Log(ret)
	t.Log(err)
}

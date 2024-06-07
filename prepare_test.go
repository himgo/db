package db

import (
	"testing"
)

func Test_StmtForExec(t *testing.T) {
	Connect("127.0.0.1", "root", "123456", "tetele_empty", "3306")

	stmt, err := StmtForUpdate("dev_tetele_net", "ttl_xshop_order", []string{"user_id = ?", "pay_time = pay_time+1"}, []string{"id=1"})

	if err != nil {
		t.Error(err)
	}

	defer stmt.Close()
	ret, err := StmtForUpdateExec(stmt, []interface{}{"1"})

	t.Log(ret)
	t.Log(err)
}

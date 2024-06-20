package db

import (
	"testing"
)

func Test_Chain(t *testing.T) {
	SetTablePrefix("spxo_")
	Connect("127.0.0.1", "test", "test", "shopxo", "3306")

	ret, err := NewQuery().Db("shopxo").Table("article").Alias("a").
		LeftJoin("article_category as c", "c.id=a.article_category_id").
		Where("a.title =?").Value("积分细则").Debug(true).List()

	t.Log(len(ret))
	t.Log(ret)
	t.Log(err)
}

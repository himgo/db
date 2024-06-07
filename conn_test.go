package db

import (
	"testing"
)

func Test_Connect(t *testing.T) {
	DBHOST := "127.0.0.1"
	DBUSER := "guzeng"
	DBPWD := "123456"
	DBNAME := "chongqing"
	DBPORT := "3306"
	err := Connect(DBHOST, DBUSER, DBPWD, DBNAME, DBPORT)

	t.Log(err)
}

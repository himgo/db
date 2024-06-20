package db

import (
	"database/sql"
	"log"

	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// 主库
var DB *sql.DB

// 从库
var SLAVER_DB *sql.DB

// db类型,默认空，如TencentDB（腾讯）,
var DB_PROVIDER string

// 表前缀
var TABLE_PREFIX string

func Connect(DBHOST, DBUSER, DBPWD, DBNAME, DBPORT string, conns ...int) error {

	var dbConnErr error
	if DBHOST != "" && DBUSER != "" && DBPWD != "" && DBPORT != "" { //&& DBNAME != ""

		for i := 0; i < 10; i++ {
			DB, dbConnErr = sql.Open("mysql", DBUSER+":"+DBPWD+"@tcp("+DBHOST+":"+DBPORT+")/"+DBNAME+"?charset=utf8mb4")
			if dbConnErr != nil {
				log.Println("ERROR", "can not connect to Database, ", dbConnErr)
				time.Sleep(time.Second * 5)
			} else {
				if len(conns) > 0 {
					DB.SetMaxOpenConns(conns[0]) //用于设置最大打开的连接数，默认值为0表示不限制
				} else {
					DB.SetMaxOpenConns(200) //默认值为0表示不限制
				}
				if len(conns) > 1 {
					DB.SetMaxIdleConns(conns[1]) //用于设置闲置的连接数
				} else {
					DB.SetMaxIdleConns(50)
				}

				DB.Ping()

				log.Println("database connected")
				DB.SetConnMaxLifetime(time.Minute * 2)
				break
			}
		}
	} else {
		return errors.New("db connection params errors")
	}
	return dbConnErr
}

func CloseConn() error {
	return DB.Close()
}

func ConnectSlaver(DBHOST, DBUSER_SLAVER, DBPWD_SLAVER, DBNAME, DBPORT string, conns ...int) error {

	log.Println("database connectting with slaver...")
	var dbConnErr error
	if DBHOST != "" && DBUSER_SLAVER != "" && DBPWD_SLAVER != "" && DBPORT != "" { //&& DBNAME != ""

		for i := 0; i < 10; i++ {
			SLAVER_DB, dbConnErr = sql.Open("mysql", DBUSER_SLAVER+":"+DBPWD_SLAVER+"@tcp("+DBHOST+":"+DBPORT+")/"+DBNAME+"?charset=utf8mb4")
			if dbConnErr != nil {
				log.Println("ERROR", "can not connect to Database, ", dbConnErr)
				time.Sleep(time.Second * 5)
			} else {
				if len(conns) > 0 {
					SLAVER_DB.SetMaxOpenConns(conns[0]) //用于设置最大打开的连接数，默认值为0表示不限制
				} else {
					SLAVER_DB.SetMaxOpenConns(200) //默认值为0表示不限制
				}
				if len(conns) > 1 {
					SLAVER_DB.SetMaxIdleConns(conns[1]) //用于设置闲置的连接数
				} else {
					SLAVER_DB.SetMaxIdleConns(50)
				}

				SLAVER_DB.Ping()

				log.Println("database connected")
				SLAVER_DB.SetConnMaxLifetime(time.Minute * 2)
				break
			}
		}
	} else {
		return errors.New("db connection params errors")
	}
	return dbConnErr
}

func CloseSlaverConn() error {
	return SLAVER_DB.Close()
}

func SetTablePrefix(pre string) {
	if pre != "" {
		TABLE_PREFIX = pre
	}
}

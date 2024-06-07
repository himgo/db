package db

import (
	"database/sql"
	"errors"
	"log"
	"strings"
)

/**
 * 准备查询
 * return Stmt error
 */
func StmtForRead(dbName, table string, title string, where []string, limit map[string]string) (*sql.Stmt, error) {

	if dbName == "" && table == "" {
		return nil, errors.New("参数错误，没有数据表")
	}

	dbName = GetDbTableName(dbName, table)

	if len(title) < 1 {
		return nil, errors.New("没有要查询内容")
	}

	var limitStr string = ""

	if limit != nil && len(limit) > 0 {
		var from, offset string = "", "" //开始

		if _, ok := limit["order"]; ok {
			limitStr += " order by " + limit["order"]
		}

		if _, ok := limit["from"]; ok {
			from = limit["from"]
		}
		if _, ok := limit["offset"]; ok {
			offset = limit["offset"]
		}
		if from != "" && offset != "" {

			limitStr += " limit " + from + "," + offset
		}

	}

	var stmt *sql.Stmt
	var err error

	if len(where) > 0 {
		// log.Println("SELECT " + title + " FROM " + dbName + " where " + strings.Join(where, " and ") + limitStr)
		stmt, err = DB.Prepare("SELECT " + title + " FROM " + dbName + " where " + strings.Join(where, " and ") + limitStr)
	} else {
		// log.Println("SELECT " + title + " FROM " + dbName + limitStr)
		stmt, err = DB.Prepare("SELECT " + title + " FROM " + dbName + limitStr)
	}

	return stmt, err
}

func CloseStmt(stmt *sql.Stmt) error {
	return stmt.Close()
}

/**
 * 执行查询列表
 * return list error
 */
func StmtForQueryList(stmt *sql.Stmt, valuelist []interface{}) ([]map[string]string, error) {

	if stmt == nil {
		return nil, errors.New("缺少必要参数")
	}

	// log.Println(valuelist...)

	rows, err := stmt.Query(valuelist...)
	defer stmt.Close()
	if err != nil {
		if stmt != nil {
			stmt.Close()
		}
		return nil, err
	}
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	var list []map[string]string
	var index string
	var rowerr error
	info := make(map[string]string)

	for rows.Next() {

		rowerr = rows.Scan(scanArgs...)

		info = make(map[string]string)
		if rowerr == nil {
			for i, col := range values {
				if col != nil {
					index = StrFirstToUpper(columns[i])
					info[index] = ToString(col)
				}
			}
		} else {
			log.Println("rows scan error", rowerr)
		}
		if len(info) > 0 {
			list = append(list, info)
		}
	}

	return list, nil
}

/**
 * 执行查询一条数据
 * return row error
 */
func StmtForQueryRow(stmt *sql.Stmt, valuelist []interface{}) (map[string]string, error) {

	if stmt == nil || len(valuelist) < 1 {
		return nil, errors.New("缺少必要参数")
	}
	rows, err := stmt.Query(valuelist...)
	defer stmt.Close()
	if err != nil {
		if stmt != nil {
			stmt.Close()
		}
		return nil, err
	}
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	var index string
	var rowerr error
	info := make(map[string]string)
	for rows.Next() {
		rowerr = rows.Scan(scanArgs...)
		if rowerr == nil {
			for i, col := range values {
				if col != nil {
					index = StrFirstToUpper(columns[i])
					info[index] = ToString(col)
				}
			}
		} else {
			log.Println("rows scan error", rowerr)
		}
	}
	if rowerr != nil {
		return info, errors.New("数据出错")
	}
	return info, nil
}

/**
 * 准备更新
 * return Stmt error
 */
func StmtForUpdate(dbName, table string, data []string, where []string) (*sql.Stmt, error) {

	if dbName == "" && table == "" {
		return nil, errors.New("参数错误，没有数据表")
	}

	dbName = GetDbTableName(dbName, table)

	if len(where) < 1 {
		return nil, errors.New("参数错误，没有更新条件")
	}

	var stmt *sql.Stmt
	var err error

	stmt, err = DB.Prepare("update " + dbName + " set " + strings.Join(data, " , ") + " where " + strings.Join(where, " and "))

	return stmt, err
}

/**
 * 执行更新
 * return is_updated error
 */
func StmtForUpdateExec(stmt *sql.Stmt, valuelist []interface{}) (int64, error) {
	res, err := stmt.Exec(valuelist...)
	if err != nil {
		return 0, errors.New("更新失败：" + err.Error())
	}

	return res.RowsAffected()
}

/**
 * 准备写入
 * return Stmt error
 */
func StmtForInsert(dbName, table string, data []string) (*sql.Stmt, error) {

	if dbName == "" && table == "" {
		return nil, errors.New("参数错误，没有数据表")
	}

	dbName = GetDbTableName(dbName, table)

	if len(data) < 1 {
		return nil, errors.New("参数错误，没有要写入的数据")
	}

	var stmt *sql.Stmt
	var err error

	stmt, err = DB.Prepare("insert into " + dbName + " set " + strings.Join(data, " , "))

	return stmt, err
}

/**
 * 执行写入
 * @return lastId error
 */
func StmtForInsertExec(stmt *sql.Stmt, valuelist []interface{}) (int64, error) {
	res, err := stmt.Exec(valuelist...)
	if err != nil {
		return 0, errors.New("创建失败：" + err.Error())
	}
	return res.LastInsertId()
}

/**
 * 使用db prepare方式查询列表
 * @param dbName
 * @param title 查询的字段名
 * @param where 查询条件
 * @param valuelist 查询的条件值
 * @param limit 查询排序
 * @param page 查询范围，可传两个值 pageNum,pageSize
 * GZ
 * 2020/05/19
 */
func GetListByStmt(dbName string, table string, title string, where []string, valuelist []interface{}, limit map[string]string, page ...int) ([]map[string]string, error) {

	if len(page) > 0 {
		pageNum, pageSize := page[0], 10
		if len(page) > 1 {
			pageSize = page[1]
		}
		limit["from"], limit["offset"] = GetPage(pageNum, pageSize)
	}

	stmt, err := StmtForRead(dbName, table, title, where, limit)

	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return StmtForQueryList(stmt, valuelist)

}

/**
 * 使用db prepare方式查询单条数据
 * @param dbName
 * @param title 查询的字段名
 * @param where 查询条件
 * @param valuelist 查询的条件值
 * @param limit 查询排序
 * GZ
 * 2020/05/19
 */
func GetDataByStmt(dbName string, table string, title string, where []string, valuelist []interface{}, limit map[string]string) (map[string]string, error) {

	stmt, err := StmtForRead(dbName, table, title, where, limit)

	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return StmtForQueryRow(stmt, valuelist)
}

/**
 * 使用db prepare修改数据
 * @param dbName
 * @param title 查询的字段名
 * @param where 查询条件
 * @param valuelist 查询的条件值
 * @param limit 查询排序
 * GZ
 * 2020/05/19
 */
func UpdateByStmt(dbName string, table string, data []string, where []string, valuelist []interface{}) (int64, error) {

	stmt, err := StmtForUpdate(dbName, table, data, where)

	if err != nil {
		return 0, err
	}

	defer stmt.Close()
	return StmtForUpdateExec(stmt, valuelist)
}

/**
 * 使用db prepare写入数据
 * @param dbName
 * @param table 表名
 * @param data 写入的字段
 * @param valuelist 写入的值
 * GZ
 * 2020/08/06
 */
func InsertByStmt(dbName string, table string, data []string, valuelist []interface{}) (int64, error) {

	stmt, err := StmtForInsert(dbName, table, data)

	if err != nil {
		return 0, err
	}

	defer stmt.Close()
	return StmtForInsertExec(stmt, valuelist)
}

/**
 * 自定义查询
 * return Stmt error
 */
func StmtForQuery(querysql string) (*sql.Stmt, error) {

	if querysql == "" {
		return nil, errors.New("参数错误，没有数据表")
	}

	var stmt *sql.Stmt
	var err error

	stmt, err = DB.Prepare(querysql)

	return stmt, err
}

/**
 * 执行自定义查询
 * @return lastId error
 */
func QueryByStmt(sql string, valuelist []interface{}) ([]map[string]string, error) {
	stmt, err := StmtForQuery(sql)

	if err != nil {
		return nil, err
	}

	defer stmt.Close()
	return StmtForQueryList(stmt, valuelist)

}

/**
 * 联表查询
 * @param dbName
 * @param tableA 表一
 * @param tableA_alias 表一别名
 * @param tableB 表二
 * @param tableB_alias 表二别名
 * @param join 联表方式
 * @param join_on 联表字段
 * @param title 查询的字段名
 * @param where 查询条件
 * @param valuelist 查询的条件值
 * @param limit 查询排序
 * @param page 查询范围，可传两个值 pageNum,pageSize
 * GZ
 * 2020/11/23
 */
func GetJoinListByStmt(dbName string, tableA, tableA_alias string, tableB, tableB_alias string, join_type, join_on string, title string, where []string, valuelist []interface{}, limit map[string]string, page ...int) ([]map[string]string, error) {

	if len(page) > 0 {
		pageNum, pageSize := page[0], 10
		if len(page) > 1 {
			pageSize = page[1]
		}
		limit["from"], limit["offset"] = GetPage(pageNum, pageSize)
	}

	if tableA_alias != "" {
		tableA = StringJoin(dbName, ".", tableA, " as ", tableA_alias)
	}

	if tableB_alias != "" {
		tableB = StringJoin(dbName, ".", tableB, " as ", tableB_alias)
	}
	table := StringJoin(tableA, " ", join_type, " join ", tableB, " on ", join_on)
	stmt, err := StmtForRead(dbName, table, title, where, limit)

	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return StmtForQueryList(stmt, valuelist)

}

/**
 * 左联表查询
 * @param dbName
 * @param tableA 表一
 * @param tableB 表二
 * @param join_on 联表字段
 * @param title 查询的字段名
 * @param where 查询条件
 * @param valuelist 查询的条件值
 * @param limit 查询排序
 * @param page 查询范围，可传两个值 pageNum,pageSize
 * GZ
 * 2021/1/27
 */
func LeftJoinListByStmt(dbName string, tableA, tableB string, join_on string, title string, where []string, valuelist []interface{}, limit map[string]string, page ...int) ([]map[string]string, error) {

	if len(page) > 0 {
		pageNum, pageSize := page[0], 10
		if len(page) > 1 {
			pageSize = page[1]
		}
		limit["from"], limit["offset"] = GetPage(pageNum, pageSize)
	}

	tableA = GetDbTableName(dbName, tableA)
	tableB = GetDbTableName(dbName, tableB)

	table := StringJoin(tableA, " left join ", tableB, " on ", join_on)
	stmt, err := StmtForRead(dbName, table, title, where, limit)

	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return StmtForQueryList(stmt, valuelist)

}

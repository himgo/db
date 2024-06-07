package db

import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"strings"
)

var stmt *sql.Stmt
var err error

type Query struct {
	dbname    string
	table     string
	alias     string
	title     string
	where     []string
	where_or  []string
	join      [][]string               //[["tablea as a","a.id=b.id","left"]]
	save_data []map[string]interface{} //批量操作的数据[["title":"a","num":1,],["title":"a","num":1,]]
	upd_field []string                 // 批量更新时需要更新的字段，为空时按除id外的字段进行更新
	data      []string
	value     []interface{}
	orderby   string
	groupby   string
	having    string
	page      int
	page_size int
	stmt      *sql.Stmt
	conn      *sql.DB
	debug     bool
	dbtype    string
}

func NewQuery() *Query {

	var conn_type *sql.DB = DB

	var db_type string = "mysql"

	return &Query{
		conn:   conn_type,
		dbtype: db_type,
	}
}

func (this *Query) Conn(conn *sql.DB) *Query {
	this.conn = conn
	return this
}
func (this *Query) Db(dbname string) *Query {
	this.dbname = dbname
	return this
}

func (this *Query) Table(tablename string) *Query {
	this.table = tablename
	return this
}
func (this *Query) Alias(tablename string) *Query {
	this.alias = tablename
	return this
}

func (this *Query) Title(title string) *Query {
	this.title = title
	return this
}
func (this *Query) Page(page int) *Query {
	this.page = page
	return this
}
func (this *Query) PageSize(page_num int) *Query {
	this.page_size = page_num
	return this
}
func (this *Query) Having(having string) *Query {
	this.having = having
	return this
}
func (this *Query) Orderby(orderby string) *Query {
	this.orderby = orderby
	return this
}
func (this *Query) Groupby(groupby string) *Query {
	this.groupby = groupby
	return this
}
func (this *Query) Where(where string) *Query {
	this.where = append(this.where, where)
	return this
}
func (this *Query) Wheres(wheres []string) *Query {
	if len(wheres) > 0 {
		this.where = append(this.where, wheres...)
	}
	return this
}
func (this *Query) WhereOr(where string) *Query {
	this.where_or = append(this.where_or, where)
	return this
}
func (this *Query) SaveData(value map[string]interface{}) *Query {
	this.save_data = append(this.save_data, value)
	return this
}
func (this *Query) SaveDatas(value []map[string]interface{}) *Query {
	this.save_data = append(this.save_data, value...)
	return this
}
func (this *Query) UpdField(value string) *Query {
	this.upd_field = append(this.upd_field, value)
	return this
}
func (this *Query) UpdFields(value []string) *Query {
	this.upd_field = append(this.upd_field, value...)
	return this
}
func (this *Query) Value(value interface{}) *Query {
	this.value = append(this.value, value)
	return this
}
func (this *Query) Values(values []interface{}) *Query {
	this.value = append(this.value, values...)
	return this
}
func (this *Query) Join(join []string) *Query {
	this.join = append(this.join, join)
	return this
}

/**
 * 左连接
 * 2023/08/10
 * gz
 */
func (this *Query) LeftJoin(table_name string, condition string) *Query {
	this.join = append(this.join, []string{table_name, condition, "left"})
	return this
}

/**
 * 右连接
 * 2023/08/10
 * gz
 */
func (this *Query) RightJoin(table_name string, condition string) *Query {
	this.join = append(this.join, []string{table_name, condition, "right"})
	return this
}

func (this *Query) Data(data string) *Query {
	this.data = append(this.data, data)
	return this
}
func (this *Query) Datas(datas []string) *Query {
	this.data = append(this.data, datas...)
	return this
}

func (this *Query) Debug(debug bool) *Query {
	this.debug = debug
	return this
}

/*
 * 清理上次查询
 */
func (this *Query) Clean() *Query {
	this.title = ""
	this.where = this.where[0:0]
	this.where_or = this.where_or[0:0]
	this.join = this.join[0:0]
	this.data = this.data[0:0]
	this.value = this.value[0:0]
	this.orderby = ""
	this.groupby = ""
	this.page = 0
	this.page_size = 0
	this.save_data = this.save_data[0:0]
	this.upd_field = this.upd_field[0:0]
	this.having = ""
	this.alias = ""
	return this
}

// 获取表格信息
func (this *Query) GetTableInfo(table string) (map[string]interface{}, error) {
	field := []string{
		"COLUMN_NAME",    //字段名
		"COLUMN_DEFAULT", //默认值
		"DATA_TYPE",      //数据类型
		"COLUMN_TYPE",    //数据类型+长度
		"COLUMN_COMMENT", //备注
		"IS_NULLABLE",    //是否为空
	}
	sql := "select `" + strings.Join(field, "`,`") + "` from information_schema.COLUMNS where table_name = ? and table_schema = ?"
	if this.conn == nil {
		this.conn = DB
	}
	stmtSql, err := this.conn.Prepare(sql)
	if err != nil {
		return nil, err
	}
	list, err := StmtForQueryList(stmtSql, []interface{}{table, this.dbname})
	if err != nil {
		return nil, err
	}
	rows := make([]interface{}, 0, len(list))
	fieldName := make([]string, 0, len(list))
	for _, item := range list {
		info := map[string]interface{}{
			"name":        "",
			"column_type": "",
			"is_null":     true,
			"data_type":   "",
			"comment":     "",
			"default":     "",
		}
		for _, k := range field {
			index := StrFirstToUpper(k)
			if v, ok := item[index]; ok {
				switch k {
				case "COLUMN_NAME":
					info["name"] = v
				case "COLUMN_DEFAULT":
					info["default"] = v
				case "DATA_TYPE":
					info["data_type"] = v
				case "COLUMN_TYPE":
					info["column_type"] = ToInt64(v)
				case "COLUMN_COMMENT":
					info["comment"] = ToInt64(v)
				case "IS_NULLABLE":
					if v == "NO" {
						info["is_null"] = false
					}
				}
			}
		}
		name := ToStr(info["name"])
		if name != "" {
			rows = append(rows, info)
			fieldName = append(fieldName, name)
		}
	}
	return map[string]interface{}{
		"field": fieldName,
		"list":  rows,
	}, nil
}

// 返回表名
func (this *Query) GetTableName(table string) string {
	return GetDbTableName(this.dbname, table)
}

// 构造子查询
func (this *Query) BuildSelectSql() (map[string]interface{}, error) {
	if this.dbname == "" && this.table == "" {
		return nil, errors.New("参数错误，没有数据表")
	}
	var table = ""
	if strings.Contains(this.table, "select ") {
		table = this.table
	} else {
		table = GetDbTableName(this.dbname, this.table, this.dbtype)
	}

	// var err error

	var sql, title string

	if this.title != "" {
		title = this.title
	} else {
		title = "*"
	}

	sql = "select "

	if DB_PROVIDER == "TencentDB" {
		sql = "/*slave*/ select "
	}

	sql = StringJoin(sql, title)

	if this.alias != "" {
		table = StringJoin(table, " as ", this.alias)
	}

	sql = StringJoin(sql, " from ", table)

	if len(this.join) > 0 {
		join_type := "left"
		for _, joinitem := range this.join {
			if len(joinitem) < 2 {
				continue
			}
			if len(joinitem) == 3 {
				join_type = joinitem[2]
			} else { //默认左连接
				join_type = "left"
			}
			sql = StringJoin(sql, " ", join_type, " join ", GetDbTableName(this.dbname, joinitem[0], this.dbtype), " on ", joinitem[1])
		}
	}
	if len(this.where) > 0 || len(this.where_or) > 0 {
		sql = StringJoin(sql, " where ")
	}
	if len(this.where) > 0 {
		sql = StringJoin(sql, " (", strings.Join(this.where, " and "), " ) ")
	}
	if len(this.where_or) > 0 {
		if len(this.where) > 0 {
			sql = StringJoin(sql, " or ", strings.Join(this.where_or, " or "))
		} else {
			sql = StringJoin(sql, strings.Join(this.where_or, " or "))
		}
	}
	if this.groupby != "" {
		sql = StringJoin(sql, " group by ", this.groupby)

	}
	if this.having != "" {
		sql = StringJoin(sql, " having ", this.having)

	}
	if this.orderby != "" {
		sql = StringJoin(sql, " order by ", this.orderby)
	}

	if this.dbtype == "mysql" && (this.page > 0 || this.page_size > 0) {

		if this.page < 1 {
			this.page = 1
		}
		if this.page_size < 1 {
			this.page_size = 10
		}
		from := strconv.Itoa((this.page - 1) * this.page_size)
		offset := strconv.Itoa(this.page_size)
		if from != "" && offset != "" {
			sql = StringJoin(sql, " limit ", from, " , ", offset)
		}
	}
	if this.debug {
		log.Println("query sql:", sql, this.value)
	}
	condition_len := 0 //所有条件数
	for _, ch2 := range sql {
		if string(ch2) == "?" {
			condition_len++
		}
	}
	if condition_len != len(this.value) {
		return nil, errors.New("参数错误，条件值错误")
	}
	return map[string]interface{}{
		"sql":   sql,
		"value": this.value,
	}, nil
}

// 拼查询sql
func (this *Query) QueryStmt() error {
	res := map[string]interface{}{}
	res, err = this.BuildSelectSql()
	if err != nil {
		return err
	}
	sql := ToStr(res["sql"])

	if SLAVER_DB != nil {
		this.conn = SLAVER_DB
	}
	// else {
	// 	this.conn = DB
	// }
	if this.conn == nil {
		this.conn = DB
	}

	stmt, err = this.conn.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼更新sql
func (this *Query) UpdateStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}
	if len(this.where) < 1 {
		return errors.New("参数错误，缺少条件")
	}

	dbName := GetDbTableName(this.dbname, this.table, this.dbtype)

	var sql string

	sql = StringJoin("update ", dbName, " set ", strings.Join(this.data, " , "))

	sql = StringJoin(sql, " where ", strings.Join(this.where, " and "))

	if this.debug {
		log.Println("update sql:", sql, this.value)
	}
	condition_len := 0 //所有条件数

	for _, ch2 := range sql {
		if string(ch2) == "?" {
			condition_len++
		}
	}
	if condition_len != len(this.value) {
		return errors.New("参数错误，条件值错误")
	}

	if this.conn == nil {
		this.conn = DB
	}

	stmt, err = this.conn.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼批量存在更新不存在插入sql
func (this *Query) UpdateAllStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}

	dbName := GetDbTableName(this.dbname, this.table)

	var sql string
	var dataSql []string                  //一组用到的占位字符
	var valSql []string                   //占位字符组
	var updSql []string                   //更新字段的sql
	var updFieldLen = len(this.upd_field) //需要更新的字段数量，为0时更新除id外添加值
	dataLen := len(this.save_data)
	if dataLen > 0 {
		//批量操作
		this.data = this.data[0:0]
		this.value = this.value[0:0]
		var dataSqlText string //占位字符组
		for i := 0; i < dataLen; i++ {
			if i == 0 {
				//第一组时分配变量空间
				fieldLen := len(this.save_data[i])
				this.data = make([]string, 0, fieldLen)
				dataSql = make([]string, 0, fieldLen)
				this.value = make([]interface{}, 0, fieldLen*dataLen)
				valSql = make([]string, 0, dataLen)
				switch updFieldLen {
				case 0:
					//预览创建数据的长度
					updSql = make([]string, 0, fieldLen)
				default:
					//按照需要更新字段数长度
					updSql = make([]string, 0, updFieldLen)
					for _, k := range this.upd_field {
						updSql = append(updSql, k+"=values("+k+")") //存储需要更新的字段
					}
				}
				for k := range this.save_data[i] {
					this.data = append(this.data, k) //存储添加的字段
					dataSql = append(dataSql, "?")   //存储需要的占位符
					if updFieldLen == 0 && k != "id" {
						updSql = append(updSql, k+"=values("+k+")") //存储需要更新的字段
					}
				}
				dataSqlText = strings.Join(dataSql, ",") //组成每组占位字符格式
			}
			for j := 0; j < len(this.data); j++ {
				this.value = append(this.value, this.save_data[i][this.data[j]]) //存储值
			}
			valSql = append(valSql, "("+dataSqlText+")") //组成占位字符组
		}
	} else {
		//添加一条（原理同上）
		fieldLen := len(this.data)
		dataSql = make([]string, 0, fieldLen)
		valSql = make([]string, 0, 1)
		switch updFieldLen {
		case 0:
			updSql = make([]string, 0, fieldLen)
		default:
			updSql = make([]string, 0, updFieldLen)
			for _, k := range this.upd_field {
				updSql = append(updSql, k+"=values("+k+")")
			}
		}
		for i := 0; i < fieldLen; i++ {
			dataSql = append(dataSql, "?")
			if updFieldLen == 0 && this.data[i] != "id" {
				updSql = append(updSql, this.data[i]+"=values("+this.data[i]+")")
			}
		}
		if updFieldLen > 0 {
			for _, k := range this.upd_field {
				updSql = append(updSql, k+"=values("+k+")")
			}
		}
		valSql = append(valSql, "("+strings.Join(dataSql, " , ")+")")
	}

	if len(this.data) == 0 {
		return errors.New("参数错误，没有字段值")
	}
	if len(this.value) == 0 {
		return errors.New("参数错误，条件值错误")
	}

	setText := " values "
	if len(valSql) > 1 {
		setText = " value "
	}
	sql = StringJoin("insert into ", dbName, " (", strings.Join(this.data, " , "), ")", setText, strings.Join(valSql, ","), " ON DUPLICATE KEY UPDATE ", strings.Join(updSql, " , "))

	if this.debug {
		log.Println("insert on duplicate key update sql:", sql, this.value)
	}
	conditionLen := 0 //所有条件数

	for _, ch2 := range sql {
		if string(ch2) == "?" {
			conditionLen++
		}
	}
	if conditionLen != len(this.value) {
		return errors.New("参数错误，条件值数量不匹配")
	}
	if this.conn == nil {
		this.conn = DB
	}

	stmt, err = this.conn.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼批量插入sql
func (this *Query) CreateAllStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}

	dbName := GetDbTableName(this.dbname, this.table)

	var sql string
	var dataSql []string //一组用到的占位字符
	var valSql []string  //占位字符组
	dataLen := len(this.save_data)
	if dataLen > 0 {
		//清空字段和值
		this.data = this.data[0:0]
		this.value = this.value[0:0]
		var dataSqlText string //占位字符组
		for i := 0; i < dataLen; i++ {
			if i == 0 {
				//第一组时分配变量空间
				fieldLen := len(this.save_data[i])
				this.data = make([]string, 0, fieldLen)
				dataSql = make([]string, 0, fieldLen)
				this.value = make([]interface{}, 0, fieldLen*dataLen)
				valSql = make([]string, 0, dataLen)
				for k := range this.save_data[i] {
					this.data = append(this.data, k) //存储字段
					dataSql = append(dataSql, "?")   //存储需要的占位符
				}
				dataSqlText = strings.Join(dataSql, ",") //组成每组占位字符格式
			}
			for j := 0; j < len(this.data); j++ {
				this.value = append(this.value, this.save_data[i][this.data[j]]) //存储值
			}
			valSql = append(valSql, "("+dataSqlText+")") //组成占位字符组
		}
	} else {
		//添加一条（原理同上）
		fieldLen := len(this.data)
		dataSql = make([]string, 0, fieldLen)
		for i := 0; i < fieldLen; i++ {
			dataSql = append(dataSql, "?")
		}
		valSql = make([]string, 0, 1)
		valSql = append(valSql, "("+strings.Join(dataSql, " , ")+")")
	}

	if len(this.data) == 0 {
		return errors.New("参数错误，字段名错误")
	}
	if len(this.value) == 0 {
		return errors.New("参数错误，条件值错误")
	}
	//通过sql关键字优化批量操作和单个操作效率
	setText := " values "
	if len(valSql) > 1 {
		setText = " value "
	}
	sql = StringJoin("insert into ", dbName, " (", strings.Join(this.data, " , "), ")", setText, strings.Join(valSql, ","))

	if this.debug {
		log.Println("insert sql:", sql, this.value)
	}
	conditionLen := 0 //所有条件数

	for _, ch2 := range sql {
		if string(ch2) == "?" {
			conditionLen++
		}
	}
	if conditionLen != len(this.value) {
		return errors.New("参数错误，条件值数量不匹配")
	}
	if this.conn == nil {
		this.conn = DB
	}

	stmt, err = this.conn.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼插入sql
func (this *Query) CreateStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}

	dbName := GetDbTableName(this.dbname, this.table, this.dbtype)

	var sql string

	sql = StringJoin("insert into ", dbName, " set ", strings.Join(this.data, " , "))

	if this.debug {
		log.Println("insert sql:", sql, this.value)
	}
	condition_len := 0 //所有条件数

	for _, ch2 := range sql {
		if string(ch2) == "?" {
			condition_len++
		}
	}
	if condition_len != len(this.value) {
		return errors.New("参数错误，条件值错误")
	}

	if this.conn == nil {
		this.conn = DB
	}

	stmt, err = this.conn.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼删除sql
func (this *Query) DeleteStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}
	if len(this.where) < 1 {
		return errors.New("参数错误，缺少条件")
	}

	dbName := GetDbTableName(this.dbname, this.table, this.dbtype)

	var sql string

	sql = StringJoin("delete from ", dbName, " where ", strings.Join(this.where, " and "))

	if this.page_size > 0 {
		sql = StringJoin(sql, " limit ", strconv.Itoa(this.page_size))
	}

	if this.debug {
		log.Println("delete sql:", sql, this.value)
	}
	condition_len := 0 //所有条件数

	for _, ch2 := range sql {
		if string(ch2) == "?" {
			condition_len++
		}
	}
	if condition_len != len(this.value) {
		return errors.New("参数错误，条件值错误")
	}

	if this.conn == nil {
		this.conn = DB
	}

	stmt, err = this.conn.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

/**
 * 执行查询列表
 * return list error
 */
func (this *Query) Select() ([]map[string]string, error) {

	return this.List()
}

/**
 * 执行查询多条数据
 * return row error
 * 2022/01/05
 */
func (this *Query) List() ([]map[string]string, error) {

	err := this.QueryStmt()
	if err != nil {
		return []map[string]string{}, err
	}

	if this.stmt == nil {
		return []map[string]string{}, errors.New("缺少必要参数")
	}

	return StmtForQueryList(this.stmt, this.value)
}

/**
 * 执行查询一条数据
 * return row error
 */
func (this *Query) Find() (map[string]string, error) {

	return this.Get()
}

/**
 * 执行查询一条数据
 * return row error
 * 2022/01/05
 */
func (this *Query) Get() (map[string]string, error) {
	this.page = 1
	this.page_size = 1
	err := this.QueryStmt()
	if err != nil {
		return map[string]string{}, err
	}

	if this.stmt == nil {
		return nil, errors.New("缺少必要参数")
	}
	return StmtForQueryRow(this.stmt, this.value)
}

/**
 * 执行更新
 * return is_updated error
 */
func (this *Query) Update() (int64, error) {

	err := this.UpdateStmt()
	if err != nil {
		return 0, err
	}

	return StmtForUpdateExec(this.stmt, this.value)
}

// 批量更新
func (this *Query) UpdateAll() (int64, error) {

	err := this.UpdateAllStmt()
	if err != nil {
		return 0, err
	}

	return StmtForUpdateExec(this.stmt, this.value)
}

/**
 * 执行删除
 * return is_delete error
 */
func (this *Query) Delete() (int64, error) {

	err := this.DeleteStmt()
	if err != nil {
		return 0, err
	}

	return StmtForUpdateExec(this.stmt, this.value)
}

/**
 * 执行写入
 * return is_insert error
 */
func (this *Query) Create() (int64, error) {

	err := this.CreateStmt()
	if err != nil {
		return 0, err
	}

	return StmtForInsertExec(this.stmt, this.value)
}

func (this *Query) CreateAll() (int64, error) {

	err := this.CreateAllStmt()
	if err != nil {
		return 0, err
	}

	return StmtForInsertExec(this.stmt, this.value)
}

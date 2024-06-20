package db

/**
 * 事务操作
 */
import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"strings"
)

type TxQuery struct {
	dbname    string
	table     string
	alias     string
	title     string
	where     []string
	where_or  []string
	join      [][]string //[["tablea as a","a.id=b.id","left"]]
	data      []string
	value     []interface{}
	save_data []map[string]interface{} //批量操作的数据[["title":"a","num":1,],["title":"a","num":1,]]
	upd_field []string                 // 批量更新时需要更新的字段，为空时按除id外的字段进行更新
	orderby   string
	groupby   string
	having    string
	page      int
	page_size int
	stmt      *sql.Stmt
	conn      *sql.DB
	tx        *sql.Tx
	debug     bool
}

func NewTxQuery() *TxQuery {

	var conn_type *sql.DB = DB

	tx, err := conn_type.Begin()
	if err != nil {
		log.Println("start tx begin error", err)
	}

	return &TxQuery{
		conn: conn_type,
		tx:   tx,
	}
}

func (this *TxQuery) Conn(conn *sql.DB) *TxQuery {
	this.conn = conn
	return this
}
func (this *TxQuery) Db(dbname string) *TxQuery {
	this.dbname = dbname
	return this
}

func (this *TxQuery) Table(tablename string) *TxQuery {
	this.table = tablename
	return this
}
func (this *TxQuery) Alias(tablename string) *TxQuery {
	this.alias = tablename
	return this
}

func (this *TxQuery) Title(title string) *TxQuery {
	this.title = title
	return this
}
func (this *TxQuery) Page(page int) *TxQuery {
	this.page = page
	return this
}
func (this *TxQuery) PageSize(page_num int) *TxQuery {
	this.page_size = page_num
	return this
}

func (this *TxQuery) Orderby(orderby string) *TxQuery {
	this.orderby = orderby
	return this
}
func (this *TxQuery) Groupby(groupby string) *TxQuery {
	this.groupby = groupby
	return this
}
func (this *TxQuery) Having(having string) *TxQuery {
	this.having = having
	return this
}
func (this *TxQuery) Where(where string) *TxQuery {
	this.where = append(this.where, where)
	return this
}
func (this *TxQuery) Wheres(wheres []string) *TxQuery {
	if len(wheres) > 0 {
		this.where = append(this.where, wheres...)
	}
	return this
}
func (this *TxQuery) WhereOr(where string) *TxQuery {
	this.where_or = append(this.where_or, where)
	return this
}
func (this *TxQuery) Value(value interface{}) *TxQuery {
	this.value = append(this.value, value)
	return this
}
func (this *TxQuery) SaveData(value map[string]interface{}) *TxQuery {
	this.save_data = append(this.save_data, value)
	return this
}
func (this *TxQuery) SaveDatas(value []map[string]interface{}) *TxQuery {
	this.save_data = append(this.save_data, value...)
	return this
}
func (this *TxQuery) UpdField(value string) *TxQuery {
	this.upd_field = append(this.upd_field, value)
	return this
}
func (this *TxQuery) UpdFields(value []string) *TxQuery {
	this.upd_field = append(this.upd_field, value...)
	return this
}
func (this *TxQuery) Values(values []interface{}) *TxQuery {
	this.value = append(this.value, values...)
	return this
}
func (this *TxQuery) Join(join []string) *TxQuery {
	this.join = append(this.join, join)
	return this
}

/**
 * 左连接
 * 2023/08/10
 * gz
 */
func (this *TxQuery) LeftJoin(table_name string, condition string, table_alias ...string) *TxQuery {
	if len(table_alias) > 0 {
		this.join = append(this.join, []string{table_name, condition, "left", table_alias[0]})
	} else {
		this.join = append(this.join, []string{table_name, condition, "left"})
	}
	return this
}

/**
 * 右连接
 * 2023/08/10
 * gz
 */
func (this *TxQuery) RightJoin(table_name string, condition string, table_alias ...string) *TxQuery {
	if len(table_alias) > 0 {
		this.join = append(this.join, []string{table_name, condition, "right", table_alias[0]})
	} else {
		this.join = append(this.join, []string{table_name, condition, "right"})
	}
	return this
}
func (this *TxQuery) Data(data string) *TxQuery {
	this.data = append(this.data, data)
	return this
}
func (this *TxQuery) Datas(datas []string) *TxQuery {
	this.data = append(this.data, datas...)
	return this
}
func (this *TxQuery) Debug(debug bool) *TxQuery {
	this.debug = debug
	return this
}

/*
 * 清理上次查询
 */
func (this *TxQuery) Clean() *TxQuery {
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

// 返回表名
func (this *TxQuery) GetTableName(table string) string {
	return GetDbTableName(this.dbname, table)
}

// 构造子查询
func (this *TxQuery) BuildSelectSql() (map[string]interface{}, error) {
	if this.dbname == "" && this.table == "" {
		return nil, errors.New("参数错误，没有数据表")
	}
	var table = ""
	if strings.Contains(this.table, "select ") {
		table = this.table
	} else {
		table = GetDbTableName(this.dbname, this.table)
	}

	var sql, title string

	if this.title != "" {
		title = this.title
	} else {
		title = "*"
	}
	sql = StringJoin("select ", title)

	if this.alias != "" {
		table = StringJoin(table, " as ", this.alias)
	}

	sql = StringJoin(sql, " from ", table)

	if len(this.join) > 0 {
		join_type := "left"
		var join_table string
		for _, joinitem := range this.join {
			if len(joinitem) < 2 {
				continue
			}
			if len(joinitem) > 2 {
				join_type = joinitem[2]
			} else { //默认左连接
				join_type = "left"
			}
			join_table = GetDbTableName(this.dbname, joinitem[0])

			if len(joinitem) > 3 {
				join_table = StringJoin(join_table, " as ", joinitem[3])
			}
			sql = StringJoin(sql, " ", join_type, " join ", join_table, " on ", joinitem[1])

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

	if this.page > 0 || this.page_size > 0 {

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

// 获取表格信息
func (this *TxQuery) GetTableInfo(table string) (map[string]interface{}, error) {
	field := []string{
		"COLUMN_NAME",    //字段名
		"COLUMN_DEFAULT", //默认值
		"DATA_TYPE",      //数据类型
		"COLUMN_TYPE",    //数据类型+长度
		"COLUMN_COMMENT", //备注
		"IS_NULLABLE",    //是否为空
	}
	sql := "select `" + strings.Join(field, "`,`") + "` from information_schema.COLUMNS where table_name = ? and table_schema = ?"

	stmtSql, err := this.tx.Prepare(sql)
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

// 拼查询sql
func (this *TxQuery) QueryStmt() error {

	res := map[string]interface{}{}
	res, err = this.BuildSelectSql()
	if err != nil {
		return err
	}
	sql := ToStr(res["sql"])

	stmt, err = this.tx.Prepare(sql + " FOR UPDATE")

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼更新sql
func (this *TxQuery) UpdateStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}
	if len(this.where) < 1 {
		return errors.New("参数错误，缺少条件")
	}

	dbName := GetDbTableName(this.dbname, this.table)

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

	stmt, err = this.tx.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼批量存在更新不存在插入sql
func (this *TxQuery) UpdateAllStmt() error {

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
	stmt, err = this.tx.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼插入sql
func (this *TxQuery) CreateStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}

	dbName := GetDbTableName(this.dbname, this.table)

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

	stmt, err = this.tx.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼批量插入sql
func (this *TxQuery) CreateAllStmt() error {

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
	if len(this.value) == 0 {
		return errors.New("参数错误，条件值错误")
	}

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

	stmt, err = this.tx.Prepare(sql)

	if err != nil {
		return err
	}

	this.stmt = stmt

	return nil
}

// 拼删除sql
func (this *TxQuery) DeleteStmt() error {

	if this.dbname == "" && this.table == "" {
		return errors.New("参数错误，没有数据表")
	}
	if len(this.where) < 1 {
		return errors.New("参数错误，缺少条件")
	}

	dbName := GetDbTableName(this.dbname, this.table)

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

	stmt, err = this.tx.Prepare(sql)

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
func (this *TxQuery) Select() ([]map[string]string, error) {

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
func (this *TxQuery) Find() (map[string]string, error) {
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
func (this *TxQuery) Update() (int64, error) {

	err := this.UpdateStmt()
	if err != nil {
		return 0, err
	}

	return StmtForUpdateExec(this.stmt, this.value)
}

// UpdateAll 批量更新(根据唯一键判断存在则更新，不存在则创建)
func (this *TxQuery) UpdateAll() (int64, error) {

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
func (this *TxQuery) Delete() (int64, error) {

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
func (this *TxQuery) Create() (int64, error) {

	err := this.CreateStmt()
	if err != nil {
		return 0, err
	}

	return StmtForInsertExec(this.stmt, this.value)
}

/**
 * 执行批量写入
 * return is_insert error
 */
func (this *TxQuery) CreateAll() (int64, error) {

	err := this.CreateAllStmt()
	if err != nil {
		return 0, err
	}

	return StmtForInsertExec(this.stmt, this.value)
}

/**
 * 提交
 */
func (this *TxQuery) Commit() error {
	return this.tx.Commit()
}

/**
 * 回滚
 */
func (this *TxQuery) Rollback() error {
	return this.tx.Rollback()
}

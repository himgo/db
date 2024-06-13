package db

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

/**
 * 检测表名
 */
func GetDbTableName(dbName, table, dbtype string) string {

	var ret string

	if strings.Contains(table, ".") {
		ret = table
	}
	if dbName != "" {
		if strings.Contains(table, ",") {
			arr := strings.Split(table, ",")
			arrStrs := make([]string, 0, len(arr))
			for _, v := range arr {
				arrStrs = append(arrStrs, StringJoin(dbName, ".", v))
			}
			ret = strings.Join(arrStrs, ",")
		} else {
			ret = StringJoin(dbName, ".", table)
		}
	} else {
		ret = table
	}

	return ret
}

func judg() []string {
	return []string{"=", ">", "<", "!=", "<=", ">="}
}

/**
 * 根据第几页计算从第几行开始
 * @param pageNum 第几页
 * @param pageSize 每页几行
 * @return from,offset 开始行数，偏移量
 */
func GetPage(pageNum, pageSize interface{}) (string, string) {

	var from string
	var offset int = ToInt(pageSize)

	var pageNumInt, pageSizeInt int = ToInt(pageNum), ToInt(pageSize)

	if pageNumInt < 1 {
		pageNumInt = 1
	}

	if pageSizeInt < 1 {
		offset = 10
		pageSizeInt = 10
	}

	from = ToString((pageNumInt - 1) * pageSizeInt)

	return from, ToString(offset)
}

/*
 * 连接多个字符串
 * 2019/05/05
 */
func StringJoin(s ...string) string {
	var build strings.Builder
	if len(s) > 0 {
		for _, v := range s {
			build.WriteString(v)
		}
	}

	return build.String()
}

/**
 * 字符串转大驼峰 ios_bbbbbbbb -> IosBbbbbbbbb
 */
func StrFirstToUpper(str string) string {
	str = strings.ReplaceAll(str, "`", "")
	temp := strings.Split(str, "_")
	var upperStr string
	for y := 0; y < len(temp); y++ {
		vv := []rune(temp[y])
		for i := 0; i < len(vv); i++ {
			if i == 0 {
				vv[i] -= 32
				upperStr += string(vv[i])
			} else {
				upperStr += string(vv[i])
			}
		}
	}
	return upperStr
}

func ToString(v interface{}) string {
	var value string
	switch v.(type) {
	case string:
		value = v.(string)
	case int:
		value = strconv.Itoa(v.(int))
	case float64:
		value = strconv.FormatFloat(v.(float64), 'f', 2, 64)
	case float32:
		value = strconv.FormatFloat(float64(v.(float32)), 'f', 2, 64)
	case int64:
		value = strconv.FormatInt(v.(int64), 10)
	case []uint8:
		value = string(v.([]uint8))
		//	case []byte:
		//		value = string(v.([]byte))
	case time.Time:
		value = v.(time.Time).Format("2006-01-02 15:04:05")
	case interface{}:
		value = v.(string)
	case nil:
		value = ""
	default:
		log.Println("参数值类型错误", v, "not in string|int|float64|interface|int64")
	}
	return strings.Trim(value, " ")
}

func ToStr(v interface{}) string {
	var value string
	switch v.(type) {
	case string:
		value = v.(string)
	case int:
		value = strconv.Itoa(v.(int))
	case float64:
		value = strconv.FormatFloat(v.(float64), 'f', 0, 64)
	case float32:
		value = strconv.FormatFloat(float64(v.(float32)), 'f', 0, 64)
	case int64:
		value = strconv.FormatInt(v.(int64), 10)
	case []uint8:
		value = string(v.([]uint8))
		//	case []byte:
		//		value = string(v.([]byte))
	case interface{}:
		value = v.(string)
	case nil:
		value = ""
	default:
		log.Println("参数值类型错误", v, "not in string|int|float64|interface|int64")
	}
	return strings.Trim(value, " ")
}

func ToInt(inter interface{}) int {
	var value int

	switch inter.(type) {

	case string:
		value, _ = strconv.Atoi(inter.(string))
	case int:
		value = inter.(int)
	case int64:
		value = int(inter.(int64))
	case float64:
		value, _ = strconv.Atoi(fmt.Sprintf("%1.0f", inter))
	case nil:
		value = 0
	case interface{}:
		value = inter.(int)
	default:
		log.Println("参数值类型错误", inter, "not in string|int|float64|interface|int64")
	}
	return value
}

func ToInt64(inter interface{}) int64 {
	var value int64

	switch inter.(type) {

	case string:
		value, _ = strconv.ParseInt(inter.(string), 10, 64)
	case int:
		value = int64(inter.(int))
	case int64:
		value = inter.(int64)
	case float64:
		value_int, _ := strconv.Atoi(fmt.Sprintf("%1.0f", inter))
		value = int64(value_int)
	case nil:
		value = 0
	case interface{}:
		if _, ok := inter.(int64); !ok {
			value = inter.(int64)
		}
	default:
		log.Println("参数值类型错误", inter, "not in string|int|float64|interface|int64")
	}
	return value
}

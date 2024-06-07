# Golang operation on MySQL/Mariadb

数据库操作

> 封装mysql/mariadb查询方法，简化逻辑

## 使用方法

#### 查询单条记录
```
dbname:=""
tablename:=""
map,err := db.NewQuery().Db(dbname).Table(tablename).
	Where("id=?").Value(1).
	Where("name=?").Value("test").
	Debug(true).//打印sql
	Find()

```

#### 查询列表
```
list,err := db.NewQuery().Db(dbname).Table(tablename).
	Where("id=?").Value(1).
	Where("name=?").Value("test").
	List()
```
#### 条件"或"
```
list,err := db.NewQuery().Db(dbname).Table(tablename).
	Where("id=?").Where("name=?").WhereOr("mobile=?").
	Value(1).Value("test").Value("22").
	List()
```


#### 使用Join联表查 
```
jointable:=""
list,err := db.NewQuery().Db(dbname).Table(tablename).
	Join([]string{jointable,tablename.id=jointable.cid,"LEFT"}).
	Where("id=?").Where("name=?").
	Value(1).Value("test").
	List()
```

#### 更新
```
ret,err := db.NewQuery().Db(dbname).Table(tablename).
	Data("name=?").Value("xxx").
	Data("depart=?").Value("test").
	Update()
```


#### 插入
```
ret,err := db.NewQuery().Db(dbname).Table(tablename).
	Data("name=?").Value("xxx").
	Data("depart=?").Value("test").
	Create()
```

#### 删除
```
ret,err := db.NewQuery().Db(dbname).Table(tablename).
	Where("name=?").Value("xxx").
	Where("depart=?").Value("test").
	Delete()
```

### 事务

> 使用事务与上述方法类似，区别将初始化方法NewQuery()换成NewTxQuery()

#### 事务更新
```
ret,err := db.NewTxQuery().Db(dbname).Table(tablename).
	Data("name=?").Value("xxx").
	Data("depart=?").Value("test").
	Update()
```
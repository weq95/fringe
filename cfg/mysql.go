package cfg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"sync"
	"time"
	"xorm.io/xorm"
	"xorm.io/xorm/log"
)

var gormDB *gorm.DB

func NewDatabase(write logger.Writer) error {
	var logLevel uint8
	var dsn = Val(func(cfg *AppCfg) interface{} {
		logLevel = cfg.Mysql.LogLevel
		return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=%t&loc=%s",
			cfg.Mysql.Username, cfg.Mysql.Password, cfg.Mysql.Addr, cfg.Mysql.DBName,
			cfg.Mysql.Charset, cfg.Mysql.ParseTime, cfg.Mysql.Loc)
	}).(string)

	var err error
	gormDB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.New(write, logger.Config{
			SlowThreshold:             0,
			LogLevel:                  logger.LogLevel(logLevel),
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		}),
	})
	if err != nil {
		return err
	}

	var sqlDB *sql.DB
	sqlDB, err = gormDB.DB()
	if err != nil {
		return err
	}

	_ = Val(func(cfg *AppCfg) interface{} {
		sqlDB.SetMaxIdleConns(int(cfg.Mysql.MaxIdleConns))
		sqlDB.SetMaxOpenConns(int(cfg.Mysql.MaxOpenConns))
		sqlDB.SetConnMaxLifetime(time.Duration(cfg.Mysql.ConnMaxLifetime) * time.Hour)
		return nil
	})

	return sqlDB.Ping()
}

func GetDB() *gorm.DB {
	return gormDB.Debug()
}

var xormDB = new(sync.Map)

type SliceAny []interface{}

func (s SliceAny) FromDB(data []byte) error {
	return json.Unmarshal(data, &s)
}

func (s SliceAny) ToDB() ([]byte, error) {
	return json.Marshal(s)
}

func Db02() *xorm.Engine {
	return Val(func(cfg *AppCfg) interface{} {
		var engine, _ = xormDB.Load(cfg.Mysql.DBName)
		return engine
	}).(*xorm.Engine)
}

// EngineMultiple 获取除默认引擎外的所有实例
func EngineMultiple() map[string]*xorm.Engine {
	var result = make(map[string]*xorm.Engine)
	var defName = Val(func(cfg *AppCfg) interface{} {
		return cfg.Mysql.DBName
	}).(string)

	xormDB.Range(func(name, engine any) bool {
		if name.(string) != defName {
			result[name.(string)] = engine.(*xorm.Engine)
		}
		return true
	})

	return result
}

// DBOperations 数据库操作
func DBOperations(dbname string, fn func(db *xorm.Engine) (any, error)) (any, error) {
	dbname = strings.ToUpper(dbname)
	if engine, ok := xormDB.Load(dbname); ok {
		return fn(engine.(*xorm.Engine))
	}

	return nil, errors.New(fmt.Sprintf("db[%s] not found", dbname))
}

func AddDBEngine(m Mysql, dbname string) (*xorm.Engine, error) {
	var err error
	if e, ok := xormDB.Load(dbname); ok {
		zap.L().Warn(fmt.Sprintf("db_name[%s] link already exists", dbname))
		_ = e.(*xorm.Engine).Close()

		xormDB.Delete(dbname)
		zap.L().Warn(fmt.Sprintf("db_name[%s] execute close link and regenerate new link", dbname))
	}

	var link = fmt.Sprintf("%s:%s@tcp(%s)/", m.Username, m.Password, m.Addr)
	var engine, err01 = xorm.NewEngine("mysql", link)
	if err01 != nil {
		return nil, err01
	}

	_, err = engine.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE 'utf8mb4_0900_ai_ci';", dbname))
	if err != nil {
		return nil, err
	}

	_ = engine.Close()
	link = fmt.Sprintf("%s%s?charset=%s&loc=%s", link, dbname, m.Charset, m.Loc)
	if engine, err = xorm.NewEngine("mysql", link); err != nil {
		return nil, err
	}

	engine.Logger().ShowSQL(log.LogLevel(m.LogLevel) <= log.LOG_INFO)
	engine.ShowSQL(log.LogLevel(m.LogLevel) <= log.LOG_INFO)
	engine.SetMaxOpenConns(int(m.MaxOpenConns))
	engine.SetMaxIdleConns(int(m.MaxIdleConns))
	engine.SetConnMaxLifetime(time.Duration(m.ConnMaxLifetime) * time.Hour)

	xormDB.Store(dbname, engine)

	return engine, engine.Ping()
}

func XOrmCloseAll() {
	xormDB.Range(func(_, db any) bool {
		_ = db.(*xorm.Engine).Close()
		return true
	})
}

type Tables struct {
	Id        int       `json:"id" xorm:"autoincr pk"`
	DBName    string    `json:"db_name" xorm:"db_name"`
	Name      string    `json:"name" xorm:"name"`
	IncrId    uint64    `json:"incr_id" xorm:"incr_id"`
	CurrSeq   uint      `json:"curr_seq" xorm:"curr_seq"`
	NextSeq   uint      `json:"next_seq" xorm:"next_seq"`
	CreatedAt time.Time `json:"created_at" xorm:"created_at"`
	UpdateAt  time.Time `json:"update_at" xorm:"update_at"`
}

func GetTablesMap(db *xorm.Engine) (map[string]map[string]Tables, error) {
	var result = make(map[string]map[string]Tables)
	var tables []Tables
	var err = db.Cols([]string{"id", "db_name", "incr_id",
		"curr_seq", "curr_seq", "next_seq"}...).Find(&tables)
	if err != nil {
		return result, err
	}

	for _, i2 := range tables {
		if _, ok := result[i2.DBName]; !ok {
			result[i2.DBName] = make(map[string]Tables)
		}

		result[i2.DBName][i2.Name] = i2
	}

	return result, nil
}

func GetTablesArray(db *xorm.Engine) (map[string][]Tables, error) {
	var result = make(map[string][]Tables)
	var tables []Tables
	var err = db.Cols([]string{"id", "db_name", "incr_id",
		"curr_seq", "curr_seq", "next_seq"}...).Find(&tables)
	if err != nil {
		return result, err
	}

	for _, i2 := range tables {
		result[i2.DBName] = append(result[i2.DBName], i2)
	}

	return result, nil
}

type DBCacheInfo struct {
	IncrId uint64 `json:"incr_id" redis:"incr_id"`
	Sync   bool   `json:"sync" redis:"sync"`
	CurrDB uint   `json:"curr" redis:"curr"`
	NextDB uint   `json:"next" redis:"next"`
}

func (d DBCacheInfo) KEY(dbname, tableName string) string {
	return fmt.Sprintf("%s.%s", dbname, tableName)
}

func (d DBCacheInfo) MarshalBinary() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DBCacheInfo) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, d)
}

var dynamicSubTable = &DBDynamicSubTable{
	status: new(sync.Map),
	lock:   new(sync.RWMutex),
}

func DynamicTable() *DBDynamicSubTable {
	return dynamicSubTable
}

type DBDynamicSubTable struct {
	status *sync.Map
	lock   *sync.RWMutex
}

const TableMaxLimit uint64 = 50000000

func (d DBDynamicSubTable) Init(startApp bool) any {
	return Val(func(cfg *AppCfg) interface{} {
		var err error
		if startApp {
			if _, err = AddDBEngine(cfg.Mysql, cfg.Mysql.DBName); err != nil {
				return err
			}

			// 获取数据库配置的分表条数
			// TableMaxLimit,没有配置使用默认配置
		}

		var dynamicDB map[string]Database
		if dynamicDB, err = DatabaseIndex(startApp); err != nil {
			return err
		}

		for _, db := range dynamicDB {
			if _, err = AddDBEngine(cfg.Mysql, db.Name); err != nil {
				return err
			}

			_ = db.UpdateType(db.Name)
		}

		return d.ScheduleTableCreation(startApp)
	})
}

func (d DBDynamicSubTable) ShardId(count uint64) uint64 {
	return (count / TableMaxLimit) + 1
}

func (d DBDynamicSubTable) CacheValue(table Tables, exits bool) map[string]any {
	var cacheValue = map[string]any{
		"sync":     false,
		"incr_id":  table.IncrId,
		"curr_seq": table.CurrSeq,
		"next_seq": table.NextSeq,
	}
	if exits {
		cacheValue["incr_id"] = 0
	}

	return cacheValue
}

// ScheduleTableCreation 计划建表
func (d *DBDynamicSubTable) ScheduleTableCreation(isInit bool) error {
	zap.L().Debug("ScheduleTableCreation 计划建表Start")
	var tables, err = GetTablesArray(Db02())
	if err != nil {
		return err
	}

	zap.L().Debug("获取 [db.tables] 数据成功", zap.Any("tables", tables))
	var r = GetRedis()
	var ctx = context.Background()

	for dbname, engine := range EngineMultiple() {
		var table, ok = tables[dbname]
		if !ok {
			zap.L().Error(fmt.Sprintf("数据库 [%s] 配置不存在", dbname))
			continue
		}

		for _, t := range table {
			var dbCache DBCacheInfo
			var cacheExits = r.Exists(ctx, dbCache.KEY(dbname, t.Name)).Val() > 0
			_ = r.HGetAll(ctx, dbCache.KEY(dbname, t.Name)).Scan(&dbCache)
			if isInit {
				if t.CurrSeq > 0 {
					var total int64
					var shardTableName = fmt.Sprintf("%s_%s", t.Name,
						fmt.Sprintf("%04d", t.CurrSeq))
					_, err = engine.Table(shardTableName).Select("id").OrderBy("id DESC").Limit(1).Get(&total)
					if err != nil {
						zap.L().Error(err.Error())
					}
					if total == 0 {
						dbCache.IncrId = uint64(total)
					}
					t.IncrId = uint64(total)
				}

				if !cacheExits || dbCache.CurrDB == 0 || dbCache.CurrDB != t.CurrSeq {
					r.HSet(ctx, dbCache.KEY(dbname, t.Name), d.CacheValue(t, false))
				}
			}
			zap.L().Debug("tables redis 缓存信息",
				zap.String("key", dbCache.KEY(dbname, t.Name)),
				zap.Bool("dbCache_exits", cacheExits),
				zap.Any("db_cache", dbCache),
			)

			if dbCache.IncrId > 0 {
				if dbCache.IncrId > t.IncrId {
					t.IncrId = dbCache.IncrId
				}
				if isInit && dbCache.IncrId == 0 {
					dbCache.IncrId = 0
				}
			}

			var status = (t.CurrSeq >= t.NextSeq-1 && float64(t.IncrId) >= float64(TableMaxLimit)*0.8) ||
				isInit && float64(t.IncrId) >= float64(TableMaxLimit)*0.8

			if !status && t.CurrSeq-1 > 0 {
				continue
			}

			if err = d.CreateTable(engine, &t); err != nil {
				zap.L().Error(fmt.Sprintf("CreateTable Err: %s", err.Error()))
				return err
			}
			d.status.Store(dbCache.KEY(dbname, t.Name), false)
			zap.L().Debug(fmt.Sprintf("[%s] 自动建表Success", t.Name))
			_ = r.HSet(ctx, dbCache.KEY(dbname, t.Name), d.CacheValue(t, cacheExits))

		}
	}

	zap.L().Info("ScheduleTableCreation 计划建表End[Success]")
	return nil
}

// GetTableName 获取动态表名
func (d *DBDynamicSubTable) GetTableName(dbname, tablePrefix string) string {
	var dbCache DBCacheInfo
	var i int8
	var r = GetRedis()
	var ctx = context.Background()
	var err error
GetRedisDBCache:
	err = r.HGetAll(ctx, dbCache.KEY(dbname, tablePrefix)).Scan(&dbCache)
	if err != nil {
		zap.L().Error(err.Error())
	}

	if dbCache.CurrDB == 0 {
		i += 1

		if i < 10 {
			goto GetRedisDBCache
		}

		dbCache.CurrDB = dbCache.NextDB - 1
		if dbCache.NextDB == 0 {
			dbCache.CurrDB = 1
		}
	}

	var tableName = fmt.Sprintf("%04d", dbCache.CurrDB)
	if dbCache.IncrId < TableMaxLimit {
		return tableName
	}
	if !(dbCache.CurrDB+1 > dbCache.NextDB) {
		return tableName
	}

	zap.L().Warn("TableMaxLimit Overflow. [GetTableName]被动建表Start",
		zap.Uint64("table_max_limit", TableMaxLimit),
		zap.String("dbname", dbname),
		zap.String("table", tablePrefix),
		zap.Any("db_cache", dbCache),
	)
	if exits, _ := d.status.Load(dbCache.KEY(dbname, tablePrefix)); exits.(bool) {
		return tableName
	}

	var t Tables
	var exits bool
	exits, err = Db02().Where("name=? AND db_name=?", tablePrefix, dbname).Get(&t)
	if err != nil {
		zap.L().Error(err.Error())
		return tableName
	}

	if !exits {
		return tableName
	}

	zap.L().Warn("table info", zap.Any("table", t))
	_, err = DBOperations(dbname, func(db *xorm.Engine) (any, error) {
		t.CurrSeq += 1
		return nil, d.CreateTable(db, &t)
	})
	if err != nil {
		zap.L().Error(err.Error())
		return tableName
	}

	d.status.Store(dbCache.KEY(dbname, tablePrefix), true)
	var cacheValue = d.CacheValue(t, true)
	zap.L().Warn("[GetTableName]被动建表End.Success",
		zap.Any("table", t),
		zap.Any("cache_value", cacheValue),
	)
	_ = r.HSet(ctx, dbCache.KEY(dbname, tablePrefix), cacheValue)
	tableName = fmt.Sprintf("%04d", t.CurrSeq)

	return tableName
}

// CreateTable 创建表，并发安全
func (d DBDynamicSubTable) CreateTable(db *xorm.Engine, t *Tables) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if t.NextSeq <= 0 {
		t.NextSeq = 1
	}

	var tableStatementMap = d.TableStatementMap()
	var tSQl, ok = tableStatementMap[t.Name]
	if !ok {
		return errors.New(fmt.Sprintf("system table %s statement not found", t.Name))
	}
	var tableName = fmt.Sprintf("%s_%s", t.Name, fmt.Sprintf("%04d", t.NextSeq))
	t.NextSeq += 1
	t.IncrId = 1
	t.UpdateAt = time.Now()
	if t.CurrSeq == 0 {
		t.CurrSeq += 1
	}

	var _, err = db.Exec(fmt.Sprintf(tSQl, tableName, 0))
	if err != nil {
		return err
	}

	_, err = Db02().ID(t.Id).Cols("incr_id", "curr_seq", "next_seq", "updated_at").Update(t)
	if err != nil {
		return err
	}

	zap.L().Debug(fmt.Sprintf("create %s success", tableName))

	return err
}

// TableStatementMap 建表语句
func (d DBDynamicSubTable) TableStatementMap() map[string]string {
	return map[string]string{
		"bill": `CREATE TABLE IF NOT EXISTS %s
(
    id            int unsigned NOT NULL AUTO_INCREMENT,
    user_id       int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
    game_id       int unsigned NOT NULL DEFAULT '0' COMMENT '游戏id',
    platform      varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '0' COMMENT '厂商名称',
    unique_key    varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT '注单id',
    bet_amount    int                                                          NOT NULL DEFAULT '0' COMMENT '下注金额，单位：分',
    win_amount    int NOT NULL DEFAULT '0' COMMENT '赢钱，单位：分',
    tax           int unsigned NOT NULL DEFAULT '0' COMMENT '税，单位：分',
    after_tax     int                                                          NOT NULL DEFAULT '0' COMMENT '税后，单位：分',
    before_amount int unsigned NOT NULL DEFAULT '0' COMMENT '操作前',
    after_amount  int unsigned NOT NULL DEFAULT '0' COMMENT '操作后',
    api_name      varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT 'api名称',
    create_at     int unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY             user_id_unique_key (user_id,unique_key)
) ENGINE=InnoDB AUTO_INCREMENT=%d DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`,

		"req_detail": `CREATE TABLE IF NOT EXISTS %s
(
    id         int unsigned NOT NULL AUTO_INCREMENT,
    user_id    int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
    mf         varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT '子厂商',
    unique_key varchar(100)                                                 NOT NULL DEFAULT '' COMMENT '请求唯一key',
    api_name   varchar(20)                                                  NOT NULL DEFAULT '' COMMENT '接口名称',
    details    json                                                         NOT NULL COMMENT '请求参数',
    created_at int unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY          user_id_and_mf (user_id,mf)
) ENGINE=InnoDB AUTO_INCREMENT=%d DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`,
	}
}

type Database struct {
	Id       int       `json:"id"`
	Name     string    `json:"name"`
	Type     int8      `json:"type"`
	UpdateAt time.Time `json:"update_at"`
}

func (d Database) UpdateType(dbname string) (err error) {
	d.Type = 1
	d.UpdateAt = time.Now()
	var db = Db02()
	if _, err = db.ID(d.Id).Cols("type", "update_at").Update(&d); err != nil {
		return
	}

	var params = func(index int, dbname string) []any {
		var result = make([]any, 0, index)
		for i := 0; i < index; i++ {
			result = append(result, dbname)
		}

		return result
	}
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO tables (incr_id, name, db_name) VALUES
                                ( 1, 'bill', '%s'),
								( 1, 'req_detail', '%s'),
;`, params(2, dbname)))

	return
}

func DatabaseIndex(b bool) (map[string]Database, error) {
	var array []Database
	var session = Db02().Where(" 1=1 ")
	if b {
		session = session.Where("type=0")
	}

	if err := session.Find(&array); err != nil {
		return nil, err
	}

	var result = make(map[string]Database, len(array))
	for _, database := range array {
		result[database.Name] = database
	}

	return result, nil
}

package cfg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
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

type SliceAny []interface{}

func (s SliceAny) FromDB(data []byte) error {
	return json.Unmarshal(data, &s)
}

func (s SliceAny) ToDB() ([]byte, error) {
	return json.Marshal(s)
}

func DB02() *xorm.Engine {
	return Val(func(cfg *AppCfg) interface{} {
		var engine, _ = GetDynamicCore().dbs.Load(cfg.Mysql.DBName)
		return engine
	}).(*xorm.Engine)
}

// EngineMultiple 获取除默认引擎外的所有实例
func EngineMultiple() map[string]*xorm.Engine {
	var result = make(map[string]*xorm.Engine)
	var defName = Val(func(cfg *AppCfg) interface{} {
		return cfg.Mysql.DBName
	}).(string)

	GetDynamicCore().dbs.Range(func(name, engine any) bool {
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
	if engine, ok := GetDynamicCore().dbs.Load(dbname); ok {
		return fn(engine.(*xorm.Engine))
	}

	return nil, errors.New(fmt.Sprintf("db[%s] not found", dbname))
}

// AddDBNewEngine 创建新的数据库链接
func (d *DynamicSyncDB) AddDBNewEngine(m Mysql, dbname string) (*xorm.Engine, error) {
	if d.creating {
		return nil, errors.New("db creating...")
	}
	d.creatlock.Lock()
	d.creating = true
	defer func() {
		d.creating = false
		d.creatlock.Unlock()
	}()
	var err error
	if e, ok := d.dbs.Load(dbname); ok {
		_ = e.(*xorm.Engine).Close()
		d.dbs.Delete(dbname)
		zap.L().Warn(fmt.Sprintf("db_name[%s] execute close link and regenerate new link", dbname))
	}

	var link = fmt.Sprintf("%s:%s@tcp(%s)/", m.Username, m.Password, m.Addr)
	var engine *xorm.Engine
	if engine, err = xorm.NewEngine("mysql", link); err != nil {
		return nil, err
	}

	var createdb = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE 'utf8mb4_0900_ai_ci';", dbname)
	if _, err = engine.Exec(createdb); err != nil {
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

	d.dbs.Store(dbname, engine)

	return engine, engine.Ping()
}

func XOrmCloseAll() {
	GetDynamicCore().dbs.Range(func(_, db any) bool {
		_ = db.(*xorm.Engine).Close()
		return true
	})
}

type Tables struct {
	Id        int       `json:"id" xorm:"autoincr pk"`
	DBName    string    `json:"db_name" xorm:"db_name"`
	Name      string    `json:"name" xorm:"name"`
	IncrId    int64     `json:"incr_id" xorm:"incr_id"`
	CurrSeq   uint      `json:"curr_seq" xorm:"curr_seq"`
	NextSeq   uint      `json:"next_seq" xorm:"next_seq"`
	CreatedAt time.Time `json:"created_at" xorm:"created_at"`
	UpdateAt  time.Time `json:"update_at" xorm:"update_at"`
}

func (t Tables) CacheValue(exits bool) map[string]any {
	var cacheValue = map[string]any{
		"sync":     false,
		"incr_id":  t.IncrId,
		"curr_seq": t.CurrSeq,
		"next_seq": t.NextSeq,
	}
	if exits {
		cacheValue["incr_id"] = 0
	}

	return cacheValue
}

func (t Tables) Update4Fields(currSeq int) (int64, error) {
	return DB02().ID(t.Id).
		Where("curr_seq=?", currSeq).
		Cols([]string{
			"incr_id",
			"curr_seq",
			"next_seq",
			"uptime",
		}...).Update(t)
}

type DBRedisCache struct {
	IncrId     int64 `json:"incr_id" redis:"incr_id"`
	Sync       bool  `json:"sync" redis:"sync"`
	CurrDB     uint  `json:"curr" redis:"curr"`
	NextDB     uint  `json:"next" redis:"next"`
	TransferAt int64 `json:"tf_at" redis:"tf_at"`
}

func (d DBRedisCache) KEY(dbname, tbname string) string {
	return fmt.Sprintf("%s.%s", dbname, tbname)
}

func (d DBRedisCache) MarshalBinary() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DBRedisCache) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, d)
}

type DynamicSyncDB struct {
	creatlock  *sync.Mutex
	selectlock *sync.Mutex
	creating   bool
	selecting  bool
	dbs        sync.Map
	maxlimit   uint64
	dyntbname  func(tbname string, currseq int) string
}

var dynamic *DynamicSyncDB

func GetDynamicCore() *DynamicSyncDB {
	if dynamic == nil {
		dynamic = &DynamicSyncDB{
			creating:   false,
			creatlock:  new(sync.Mutex),
			selecting:  false,
			selectlock: new(sync.Mutex),
			dbs:        sync.Map{},
			maxlimit:   0,
			dyntbname: func(tbname string, currseq int) string {
				return fmt.Sprintf("%s_%04d", tbname, currseq)
			},
		}
	}

	return dynamic
}

// GetTableLimitCfg 获取配置文件中表存储最大条数
func (d *DynamicSyncDB) GetTableLimitCfg(tbname, field string) uint64 {
	if d.maxlimit > 0 {
		return d.maxlimit
	}
	if tbname == "" || field == "" {
		d.maxlimit = uint64(20000000)
		return d.maxlimit
	}

	var maxlimit uint64
	var _, err = d.DBOperations(tbname, func(db *xorm.Engine) (any, error) {
		return db.SQL("SELECT `%s` FROM `%s` LIMIT 1;", tbname, field).Get(&maxlimit)
	})
	if err != nil {
		zap.L().Error(err.Error())
		maxlimit = uint64(20000000)
	}
	if maxlimit > 0 {
		d.maxlimit = maxlimit
	}

	return d.maxlimit
}

// DBOperations 获取数据库链接
func (d *DynamicSyncDB) DBOperations(dbname string, fn func(db *xorm.Engine) (any, error)) (any, error) {
	dbname = strings.ToUpper(dbname)
	if engine, ok := d.dbs.Load(dbname); ok {
		return fn(engine.(*xorm.Engine))
	}

	return nil, errors.New(fmt.Sprintf("db[%s] not found", dbname))
}

// SelectNewDBCfg 新数据库配置
func (d *DynamicSyncDB) SelectNewDBCfg(b bool) (map[string]Database, error) {
	var array []Database
	var session = DB02().Where(" 1=1 ")
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

// DynTableInit 初始化
func (d *DynamicSyncDB) DynTableInit(startapp bool) any {
	return Val(func(cfg *AppCfg) interface{} {
		var err error
		if startapp {
			if _, err = d.AddDBNewEngine(cfg.Mysql, cfg.Mysql.DBName); err != nil {
				return err
			}
		}

		var newlinks map[string]Database
		if newlinks, err = d.SelectNewDBCfg(startapp); err != nil {
			return err
		}

		for _, db := range newlinks {
			if _, err = d.AddDBNewEngine(cfg.Mysql, db.Name); err != nil {
				return err
			}

			_ = db.UpdateType(db.Name)
		}

		return d.ScheduleTableCreation(context.TODO())
	})
}

func (d *DynamicSyncDB) GetTables(db *xorm.Engine) (map[string][]Tables, error) {
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

// GetDynTName 动态获取表名
func (d *DynamicSyncDB) GetDynTName(dbname, tbname string) string {
	var cache DBRedisCache
	var i int8
	var r = GetRedis()
	var ctx = context.Background()
	var err error
GetRedisDBCache:
	if err = r.HGetAll(ctx, cache.KEY(dbname, tbname)).Scan(&cache); err != nil {
		zap.L().Error(err.Error())
	}

	if cache.CurrDB == 0 {
		i += 1
		if i < 5 {
			goto GetRedisDBCache
		}

		cache.CurrDB = cache.NextDB - 1
		if cache.NextDB == 0 {
			cache.CurrDB = 1
		}
	}

	var dyname = d.dyntbname(tbname, int(cache.CurrDB))
	if uint64(cache.IncrId) < d.GetTableLimitCfg("", "") {
		return dyname
	}

	d.selectlock.Lock()
	if d.selecting {
		d.selectlock.Unlock()
		return dyname
	}
	d.selecting = true
	d.selectlock.Unlock()
	defer func() {
		d.selectlock.Lock()
		d.selecting = false
		d.selectlock.Unlock()
	}()
	zap.L().Warn("Redis TableMaxLimit Overflow. [ GetDynTName ]",
		zap.Uint64("max_limit", d.GetTableLimitCfg("", "")),
		zap.String("dbname", dbname),
		zap.String("table", tbname),
		zap.Any("cache", cache),
	)

	var result Tables
	var exits bool
	if exits, err = DB02().Where("name=? AND db_name=?", tbname, dbname).Get(&result); err != nil {
		zap.L().Error("[ GetDynTName ] Exec Err", zap.Error(err))
		return dyname
	}

	if !exits {
		zap.L().Error("[ GetDynTName ] Exec Err. Table Not Found", zap.String("dbname", dbname), zap.String("tbname", tbname))
		return dyname
	}

	_, err = DBOperations(dbname, func(db *xorm.Engine) (any, error) {
		return nil, d.CreateTableSQL(context.TODO(), db, &result, r, cache)
	})
	if err != nil {
		zap.L().Error("[ GetDynTName ] Exec Err", zap.Error(err))
		return dyname
	}

	dyname = d.dyntbname(tbname, int(cache.CurrDB))
	zap.L().Warn("[ GetDynTName ] Successful.", zap.String("dbname", dbname), zap.String("tbname", tbname))
	return dyname

}

// ScheduleTableCreation 定时任务｜计划建表
func (d *DynamicSyncDB) ScheduleTableCreation(ctx context.Context) error {
	var tables, err = d.GetTables(DB02())
	if err != nil {
		return err
	}

	var r = GetRedis()
	for dbname, engine := range EngineMultiple() {
		var table, ok = tables[dbname]
		if !ok {
			zap.L().Error(fmt.Sprintf("数据库 [%s] 配置不存在", dbname))
			continue
		}

		for _, t := range table {
			var cache DBRedisCache
			_ = r.HGetAll(ctx, cache.KEY(dbname, t.Name)).Scan(&cache)
			if err = d.CreateTableSQL(ctx, engine, &t, r, cache); err != nil {
				return err
			}
		}
	}

	return nil
}

// CreateTableSQL 三种情况：
// 1. 表不存在，创建且不能切换插入表
// 2. 数据量到达80%及以上且新表未被创建，不切换插入表
// 3. 数据量到达100%，必须切换插入表
func (d *DynamicSyncDB) CreateTableSQL(ctx context.Context, db *xorm.Engine, tb *Tables, rds *redis.Client, cache DBRedisCache) error {
	d.creatlock.Lock()
	if d.creating {
		d.creatlock.Unlock()
		return nil
	}
	d.creating = true
	d.creatlock.Unlock()
	defer func() {
		d.creatlock.Lock()
		d.creating = false
		d.creatlock.Unlock()
	}()
	var sqlstr, ok = d.SystemDynTSQL()[tb.Name]
	if !ok {
		return errors.New("system create table( " + tb.Name + " ) sql not found")
	}

	// 获取表数据，并且判断当前表是否存在
	var total int64
	var tbname = d.dyntbname(tb.Name, int(tb.CurrSeq))
	var exits, err = db.Table(tbname).Select("id").Get(&total)
	if err != nil {
		return err
	}

	var maxlimit = d.GetTableLimitCfg("config", "total")
	var currseq = tb.CurrSeq
	if false == exits { //表不存在
		if cache.CurrDB > 0 {
			tb.CurrSeq = cache.CurrDB //数据插入时，表名从获取
		}
		if tb.CurrSeq <= 0 {
			tb.CurrSeq = 1
		}
		tb.NextSeq = tb.CurrSeq
		tbname = d.dyntbname(tb.Name, int(tb.CurrSeq))
	} else if uint64(total) >= maxlimit { //表中数据达到最大条数
		tb.IncrId = 0
		tb.CurrSeq += 1
		tb.NextSeq = tb.CurrSeq + 1
		cache.Sync = false
		tbname = d.dyntbname(tb.Name, int(tb.CurrSeq))
	} else if float64(total) >= float64(maxlimit)*0.8 { //表中数据达到80%阈值
		if cache.Sync && tb.CurrSeq+1 == tb.NextSeq { //表已经创建
			return err
		}
		tb.NextSeq = tb.CurrSeq + 1
		tb.IncrId = total
		tbname = d.dyntbname(tb.Name, int(tb.NextSeq))
	} else {
		zap.L().Debug("_NOT_CREATE_TABLE_",
			zap.Any("tb", tb),
			zap.Any("cache", cache),
			zap.Int64("total", total),
		)
		return err
	}

	sqlstr = fmt.Sprintf(sqlstr, tbname)
	zap.L().Debug("_CREATE_TABLE_SQL_",
		zap.String("sql", sqlstr),
		zap.Any("tb", tb),
		zap.Any("cache", cache),
		zap.Int64("total", total),
	)
	if _, err = db.Exec(sqlstr); err != nil {
		return err
	}
	tb.CreatedAt = time.Now()

	cache.IncrId = tb.IncrId
	cache.CurrDB = tb.CurrSeq
	cache.NextDB = tb.NextSeq
	cache.TransferAt = tb.CreatedAt.Unix()
	err = rds.HSet(ctx, cache.KEY(tb.DBName, tb.Name), cache).Err()
	zap.L().Warn(fmt.Sprintf("create %s success.", tbname), zap.Error(err))

	_, err = tb.Update4Fields(int(currseq))
	return err
}

func (_ *DynamicSyncDB) SystemDynTSQL() map[string]string {
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
    created_at    int unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY             user_id_unique_key (user_id,unique_key),
	KEY 			platform_index_key (platform)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`,

		"detail": `CREATE TABLE IF NOT EXISTS %s
(
    id         int unsigned NOT NULL AUTO_INCREMENT,
    user_id    int unsigned NOT NULL DEFAULT '0' COMMENT '用户id',
    mf         varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '' COMMENT '子厂商',
    tx_key     varchar(200) NOT NULL DEFAULT '' COMMENT '注单key',
    unique_key varchar(100)                                                 NOT NULL DEFAULT '' COMMENT '请求唯一key',
    api_name   varchar(20)                                                  NOT NULL DEFAULT '' COMMENT '接口名称',
    details    json                                                         NOT NULL COMMENT '请求参数',
    created_at int unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY user_id_and_tx_key (user_id,tx_key,api_name) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`,
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
	var db = DB02()
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
	_, err = db.Exec(fmt.Sprintf(`
INSERT INTO tables (incr_id, name, db_name) VALUES
( 0, 'bill', '%s'),
( 0, 'req_detail', '%s')
`, params(2, dbname)))

	return
}

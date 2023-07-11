package cfg

import (
	"database/sql"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"time"
)

var db *gorm.DB

func NewDatabase() error {
	var logLevel uint8
	var dsn = Val(func(cfg *AppCfg) interface{} {
		logLevel = cfg.Mysql.LogLevel
		return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=%t&loc=%s",
			cfg.Mysql.Username, cfg.Mysql.Password, cfg.Mysql.Addr, cfg.Mysql.DBName,
			cfg.Mysql.Charset, cfg.Mysql.ParseTime, cfg.Mysql.Loc)
	}).(string)

	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.New(NewLogger(), logger.Config{
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
	sqlDB, err = db.DB()
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
	return db.Debug()
}

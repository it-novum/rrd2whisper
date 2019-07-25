package oitcdb

import (
	"github.com/it-novum/rrd2whisper/logging"
	"github.com/go-sql-driver/mysql"
)

type mysqlLogger struct {}

func (lg *mysqlLogger) Print(v ...interface{}) {
	logging.LogDisplayV(v...)
}

func init() {
	mysql.SetLogger(&mysqlLogger{})
}
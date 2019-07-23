package oitcdb

import (
	"log"
	"github.com/go-sql-driver/mysql"
)

type mysqlLogger struct {}

func (lg *mysqlLogger) Print(v ...interface{}) {
	log.Print(v...)
}

func init() {
	mysql.SetLogger(&mysqlLogger{})
}
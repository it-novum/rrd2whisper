package main

import (
	"database/sql"
	"fmt"
	"github.com/go-ini/ini"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type oitcDB struct {
	db *sql.DB
}

func newOitcDB(dsn string, ini string) (*oitcDB, error) {
	var err error
	oitc := new(oitcDB)
	if dsn == "" {
		if dsn, err = readMySQLINI(ini); err != nil {
			return nil, err
		}
	}
	if oitc.db, err = sql.Open("mysql", dsn); err != nil {
		return nil, err
	}
	if err = oitc.db.Ping(); err != nil {
		return nil, err
	}
	return oitc, nil
}

func readMySQLINI(path string) (string, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return "", fmt.Errorf("could not read mysql ini file: %s", err)
	}
	sec := cfg.Section("client")
	if sec == nil {
		return "", fmt.Errorf("no client section in mysql ini file")
	}
	var (
		host     string
		port     string
		user     string
		password string
		database string
	)

	if host, err = readIniString(sec, "host"); err != nil {
		return "", err
	}
	if port, err = readIniString(sec, "port"); err != nil {
		port = "3306"
	}
	if user, err = readIniString(sec, "user"); err != nil {
		return "", err
	}
	if password, err = readIniString(sec, "password"); err != nil {
		return "", err
	}
	if database, err = readIniString(sec, "database"); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, database), nil
}

func readIniString(sec *ini.Section, name string) (string, error) {
	k := sec.Key(name)
	if k == nil {
		return "", fmt.Errorf("Could not find %s in client section of mysql ini", name)
	}
	return k.String(), nil
}

func (oitc *oitcDB) close() error {
	return oitc.db.Close()
}

func (oitc *oitcDB) fetchPerfdata(servicename string) (string, error) {
	row := oitc.db.QueryRow(
		"SELECT nagios_objects.object_id, nagios_servicechecks.perfdata, MAX(nagios_servicechecks.start_time) AS start_time, nagios_objects.name2 "+
			"FROM nagios_objects INNER JOIN nagios_servicechecks ON nagios_servicechecks.service_object_id = nagios_objects.object_id "+
			"WHERE nagios_objects.name2 = ? "+
			"AND nagios_servicechecks.perfdata IS NOT NULL "+
			"GROUP BY nagios_objects.object_id", servicename)
	var objectID int64
	var perfdata string
	var startTime time.Time
	var uuid string
	err := row.Scan(&objectID, &perfdata, &startTime, &uuid)
	switch {
	case err == sql.ErrNoRows:
		return "", nil
	case err != nil:
		return "", fmt.Errorf("query error: %v", err)
	default:
		return perfdata, nil
	}
}

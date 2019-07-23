package oitcdb

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql" // ensure mysql driver is loaded
	"log"
	"time"
)

// OITC can be used to connect to the oitc database
type OITC struct {
	db    *sql.DB
	retry int
	ctx   context.Context
}

// NewOITC connects to the mysql server
func NewOITC(ctx context.Context, dsn, ini string, retry int) (*OITC, error) {
	var err error
	oitc := &OITC{
		retry: retry,
		ctx:   ctx,
	}
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

// Close closes the underlying database connection
func (oitc *OITC) Close() error {
	return oitc.db.Close()
}

func (oitc *OITC) queryPerfdata(servicename string) (string, error) {
	row := oitc.db.QueryRowContext(
		oitc.ctx,
		"SELECT nagios_objects.object_id, nagios_servicechecks.perfdata, MAX(nagios_servicechecks.start_time) AS start_time, nagios_objects.name2 "+
			"FROM nagios_objects INNER JOIN nagios_servicechecks ON nagios_servicechecks.service_object_id = nagios_objects.object_id "+
			"WHERE nagios_objects.name2 = ? "+
			"AND nagios_servicechecks.perfdata IS NOT NULL "+
			"GROUP BY nagios_objects.object_id",
		servicename)

	var (
		objectID  int64
		perfdata  string
		startTime time.Time
		uuid      string
	)
	err := row.Scan(&objectID, &perfdata, &startTime, &uuid)
	switch {
	case err == sql.ErrNoRows:
		return "", nil
	case err != nil:
		return "", err
	default:
		return perfdata, nil
	}
}

// FetchPerfdata quries the database for the perfdata of the specified service
// servicename must be the UUID of the server
// if the service doesn't have perfdata it returns an empty string
func (oitc *OITC) FetchPerfdata(servicename string) (string, error) {
	var res string
	var err error
	for currentTry := 0; currentTry < oitc.retry; currentTry++ {
		res, err = oitc.queryPerfdata(servicename)
		if err != nil {
			time.Sleep(time.Second)
			log.Printf("lost connection to mysql server -> retry\n")
		} else {
			return res, err
		}
	}
	return res, err
}

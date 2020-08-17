package oitcdb

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql" // ensure mysql driver is loaded
)

// OITC can be used to connect to the oitc database
type OITC struct {
	db    *sql.DB
	retry int
	ctx   context.Context
}

// UUIDToPerfdata is map of uuid to perfdata line from the database
type UUIDToPerfdata map[string]string

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

// V3QueryPerfdata returns a key value map for all services that have perfdata for evaluation of perfdata names
func (oitc *OITC) V3QueryPerfdata() (UUIDToPerfdata, error) {
	result := make(UUIDToPerfdata, 0)

	rows, err := oitc.db.QueryContext(oitc.ctx, `SELECT nagios_objects.object_id, nagios_servicechecks.perfdata, MAX(nagios_servicechecks.start_time) AS start_time, nagios_objects.name2 FROM nagios_objects INNER JOIN nagios_servicechecks ON nagios_servicechecks.service_object_id = nagios_objects.object_id WHERE  nagios_servicechecks.perfdata IS NOT NULL AND nagios_servicechecks.perfdata != "" GROUP BY nagios_objects.object_id`)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var (
			objectID  int64
			perfdata  string
			startTime []uint8
			uuid      string
		)
		if err := rows.Scan(&objectID, &perfdata, &startTime, &uuid); err != nil {
			return nil, err
		}
		result[uuid] = perfdata
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// V4QueryPerfdata returns a key value map for all services that have perfdata for evaluation of perfdata names
func (oitc *OITC) V4QueryPerfdata() (UUIDToPerfdata, error) {
	result := make(UUIDToPerfdata, 0)

	rows, err := oitc.db.QueryContext(oitc.ctx, `SELECT perfdata, MAX(start_time) AS start_time, service_description FROM statusengine_servicechecks WHERE  perfdata IS NOT NULL AND perfdata != "" GROUP BY statusengine_servicechecks.service_description`)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var (
			perfdata  string
			startTime []uint8
			uuid      string
		)
		if err := rows.Scan(&perfdata, &startTime, &uuid); err != nil {
			return nil, err
		}
		result[uuid] = perfdata
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

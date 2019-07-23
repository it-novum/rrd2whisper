package main

import (
	"context"
	"github.com/it-novum/rrd2whisper/oitcdb"
	"flag"
	"fmt"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type Cli struct {
	sourceDirectory string
	repDirectory    string
	destDirectory   string
	includeCorrupt  bool
	maxAge          int64
	limit           int
	workers         int
	retention       string
	checkOnly       bool
	noMerge         bool
	mysqlDSN        string
	mysqlINI        string
	mysqlRetry		int
	logfile         string
	nosql           bool
}

func parseCli() (*Cli, error) {
	var err error

	cli := new(Cli)
	flag.StringVar(&cli.sourceDirectory, "source", "", "Path to source directory file tree of rrd files")
	flag.StringVar(&cli.destDirectory, "dest", "", "Destination of file tree for whisper")
	flag.StringVar(&cli.repDirectory, "rep", "", "Path where replaced whisper files are stored")
	flag.BoolVar(&cli.includeCorrupt, "include-corrupt", false, "Include rrd files that could not be updated")
	flag.Int64Var(&cli.maxAge, "max-age", 1209600, "Maximum age of an rrd file to be included (in seconds since last update, default 2 weeks, 0=all)")
	flag.IntVar(&cli.limit, "limit", 0, "Limit number of rrd's in one step, 0=unlimited")
	flag.IntVar(&cli.workers, "workers", runtime.NumCPU(), "Number of workers running parallel")
	flag.StringVar(&cli.retention, "retention", "60s:365d", "retention for whisper files")
	flag.BoolVar(&cli.checkOnly, "check", false, "do not convert, only check for xml files")
	flag.BoolVar(&cli.noMerge, "no-merge", false, "don't try to merge data if destination directory and whisper file exists")
	flag.StringVar(&cli.logfile, "logfile", "/var/log/rrd2whisper.log", "Path to logfile")
	flag.StringVar(&cli.mysqlDSN, "mysql-dsn", "", "mysql connection dsn (overwrites -mysql-ini, see https://github.com/go-sql-driver/mysql#dsn-data-source-name)")
	flag.StringVar(&cli.mysqlINI, "mysql-ini", "/etc/openitcockpit/mysql.cnf", "path to mysql ini with connection credentials")
	flag.BoolVar(&cli.nosql, "no-sql", false, "Don't query the database for correct perfdata names")
	flag.IntVar(&cli.mysqlRetry, "mysql-retry", 30, "retry N times if connection to mysql server is lost with 1s delay")
	flag.Parse()

	if cli.workers <= 0 {
		cli.workers = 1
	}

	if _, err = os.Stat(cli.sourceDirectory); os.IsNotExist(err) {
		return cli, fmt.Errorf("source directory does not exist")
	}
	if cli.sourceDirectory, err = filepath.Abs(cli.sourceDirectory); err != nil {
		return cli, fmt.Errorf("could not get absolute path of source directory: %s", err)
	}
	if cli.includeCorrupt {
		fmt.Println("Converting corrupt rrd files! This usually doesn't make any sense and produces only garbage.")
	}
	if !cli.nosql && cli.mysqlDSN == "" {
		if _, err = os.Stat(cli.mysqlINI); os.IsNotExist(err) {
			return cli, fmt.Errorf("mysql ini does not exist and no dsn is specified")
		}
	}
	if cli.mysqlRetry <= 0 {
		cli.mysqlRetry = 1
	}
	if !cli.checkOnly {
		if cli.destDirectory == "" {
			return cli, fmt.Errorf("need -dest for whisper files output")
		}
		if cli.repDirectory == "" {
			if _, err = os.Stat(cli.destDirectory); !os.IsNotExist(err) {
				return cli, fmt.Errorf("if the destination directory already exists, you MUST specify a -rep directory")
			}
		}
	}

	return cli, nil
}

func logAndPrintf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	log.Printf(format, v...)
}

func logAndFatalf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	log.Fatalf(format, v...)
}

func main() {
	cli, err := parseCli()
	if err != nil {
		log.Fatalln(err)
	}
	err = initGlobals(cli)
	if err != nil {
		log.Fatalln(err)
	}
	lf, err := os.OpenFile(cli.logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Could not open log file: %s", err)
	}
	defer lf.Close()
	log.SetOutput(lf)

	var oitc *oitcdb.OITC
	if !cli.nosql {
		oitc, err = oitcdb.NewOITC(context.Background(), cli.mysqlDSN, cli.mysqlINI, cli.mysqlRetry)
		if err != nil {
			logAndFatalf("could not connect to mysql: %s\n", err)
		}
		defer oitc.Close()
	}


	logAndPrintf("Search %s for xml perfdata files\n", cli.sourceDirectory)
	workdata := gatherWorkdata(cli)
	logAndPrintf("Found: %d Todo: %d After Limit: %d Too Old: %d Corrupt: %d\n", workdata.foundTotal, workdata.foundTodo, workdata.finalTodo, workdata.tooOld, workdata.corrupt)
	if workdata.brokenXMLCount > 0 {
		logAndPrintf("Found %d broken xml files\n", workdata.brokenXMLCount)
	}
	if cli.checkOnly {
		return
	}

	pb := mpb.New()
	bar := pb.AddBar(
		int64(workdata.finalTodo),
		mpb.PrependDecorators(decor.CountersNoUnit("%d / %d", decor.WCSyncWidth)),
		mpb.AppendDecorators(decor.Percentage()),
	)

	var wg sync.WaitGroup

	jobs := make(chan *XmlNagios, cli.workers+1)

	for i := 0; i < cli.workers; i++ {
		wg.Add(1)
		go func() {
			for job := range jobs {
				err := convertRrd(job, cli.destDirectory, cli.repDirectory, !cli.noMerge, oitc)
				if err != nil {
					log.Printf("Error: Could not convert rrd file %s: %s", job.RrdPath, err)
				} else {
					log.Printf("Successfully converted %s to whisper", job.RrdPath)
				}
				bar.Increment()
			}
			wg.Done()
		}()
	}
	wg.Add(1)
	go func() {
		for _, xmlFile := range workdata.xmlFiles {
			jobs <- xmlFile
		}
		close(jobs)
		wg.Done()
	}()

	wg.Wait()
}

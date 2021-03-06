package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/it-novum/rrd2whisper/converter"
	"github.com/it-novum/rrd2whisper/logging"
	"github.com/it-novum/rrd2whisper/oitcdb"
	"github.com/it-novum/rrd2whisper/rrdpath"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

var (
	// Version number
	Version string
)

type commandLine struct {
	sourceDirectory  string
	archiveDirectory string
	destDirectory    string
	tempDirectory    string
	includeCorrupt   bool
	maxAge           int64
	limit            int
	parallel         int
	retention        string
	checkOnly        bool
	noMerge          bool
	mysqlDSN         string
	mysqlINI         string
	mysqlRetry       int
	logfile          string
	nosql            bool
	version          bool
	deleteRRD        bool
	oitcVersion      int
	sqlCache         string
	onlySQLCache     bool
}

func parseCli() (*commandLine, error) {
	var err error

	cli := new(commandLine)
	flag.StringVar(&cli.sourceDirectory, "source", "/opt/openitc/nagios/share/perfdata", "Path to source directory file tree of rrd files")
	flag.StringVar(&cli.destDirectory, "destination", "/var/lib/graphite/whisper/openitcockpit", "Destination of file tree for whisper")
	flag.StringVar(&cli.archiveDirectory, "archive", "/var/backups/old-whisper-files", "Path where replaced whisper files are stored")
	flag.StringVar(&cli.tempDirectory, "tmp-dir", "/tmp", "Alternative path to store temporary files")
	flag.BoolVar(&cli.includeCorrupt, "include-corrupt", false, "Include rrd files that could not be updated")
	flag.Int64Var(&cli.maxAge, "max-age", 1209600, "Maximum age of an rrd file to be included (in seconds since last update, default 2 weeks, 0=all)")
	flag.IntVar(&cli.limit, "limit", 0, "Limit number of rrd's in one step, 0=unlimited")
	flag.IntVar(&cli.parallel, "parallel", runtime.NumCPU(), "Number of files processed in parallel")
	flag.StringVar(&cli.retention, "retention", "60s:365d", "retention for whisper files")
	flag.BoolVar(&cli.checkOnly, "check", false, "do not convert, only check for xml files")
	flag.BoolVar(&cli.noMerge, "no-merge", false, "don't try to merge data if destination directory and whisper file exists")
	flag.StringVar(&cli.logfile, "logfile", "/var/log/rrd2whisper.log", "Path to logfile")
	flag.StringVar(&cli.mysqlDSN, "mysql-dsn", "", "mysql connection dsn (overwrites -mysql-ini, see https://github.com/go-sql-driver/mysql#dsn-data-source-name)")
	flag.StringVar(&cli.mysqlINI, "mysql-ini", "/etc/openitcockpit/mysql.cnf", "path to mysql ini with connection credentials")
	flag.BoolVar(&cli.nosql, "no-sql", false, "Don't query the database for correct perfdata names")
	flag.IntVar(&cli.mysqlRetry, "mysql-retry", 30, "retry N times if connection to mysql server is lost with 1s delay")
	flag.BoolVar(&cli.version, "version", false, "show version and exit")
	flag.BoolVar(&cli.deleteRRD, "delete-rrd", false, "delete rrd file after convertion")
	flag.IntVar(&cli.oitcVersion, "oitc-version", 3, "either 3 or 4, used for only for sql queries")
	flag.StringVar(&cli.sqlCache, "sql-cache", "", "Path to sql cache file. If -no-sql is specified and the file exists it will be used if possible. The file will be created if -no-sql is not specified.")
	flag.BoolVar(&cli.onlySQLCache, "only-sql-cache", false, "If set, it will only create the sql cache file and exit")
	flag.Parse()

	if Version == "" {
		Version = "dev"
	}

	if cli.version {
		fmt.Println("Version: ", Version)
		os.Exit(0)
	}

	if cli.parallel <= 0 {
		cli.parallel = 1
	}

	if !(cli.oitcVersion >= 3 && cli.oitcVersion <= 4) {
		logging.LogFatal("invalid oitc version")
		fmt.Println("invalid oitc version")
	}

	if cli.nosql && cli.onlySQLCache {
		logging.LogFatal("-no-sql and -only-sql-cache specified")
	}

	if cli.onlySQLCache && cli.sqlCache == "" {
		logging.LogFatal("-sql-cache is required for -only-sql-cache")
	}

	if !cli.onlySQLCache {
		if _, err = os.Stat(cli.sourceDirectory); os.IsNotExist(err) {
			return cli, fmt.Errorf("source directory does not exist")
		}
		if cli.sourceDirectory, err = filepath.Abs(cli.sourceDirectory); err != nil {
			return cli, fmt.Errorf("could not get absolute path of source directory: %s", err)
		}
		if cli.includeCorrupt {
			fmt.Println("Converting corrupt rrd files! This usually doesn't make any sense and produces only garbage.")
		}
	}

	if !cli.nosql && cli.mysqlDSN == "" {
		if _, err = os.Stat(cli.mysqlINI); os.IsNotExist(err) {
			return cli, fmt.Errorf("mysql ini does not exist and no dsn is specified")
		}
	}
	if cli.mysqlRetry <= 0 {
		cli.mysqlRetry = 1
	}
	if !cli.checkOnly && !cli.onlySQLCache {
		if cli.destDirectory == "" {
			return cli, fmt.Errorf("need -dest for whisper files output")
		}
		if cli.archiveDirectory == "" {
			if _, err = os.Stat(cli.destDirectory); !os.IsNotExist(err) {
				return cli, fmt.Errorf("if the destination directory already exists, you MUST specify a -rep directory")
			}
		}
	}

	return cli, nil
}

type barIncrementor struct {
	bar *mpb.Bar
}

func (bi *barIncrementor) Visit(_ *rrdpath.RrdSet, duration time.Duration, _ error) {
	bi.bar.Increment(duration)
}

func queryDB(ctx context.Context, cli *commandLine) (oitcdb.UUIDToPerfdata, error) {
	var (
		perfdata oitcdb.UUIDToPerfdata
		oitc     *oitcdb.OITC
		err      error
	)

	oitc, err = oitcdb.NewOITC(ctx, cli.mysqlDSN, cli.mysqlINI, cli.mysqlRetry)
	if err != nil {
		logging.LogFatal("could not connect to mysql: %s", err)
	}
	defer oitc.Close()

	if cli.oitcVersion == 3 {
		perfdata, err = oitc.V3QueryPerfdata()
		if err != nil {
			logging.LogFatal("could not query database perfdata: ", err)
		}
	} else {
		perfdata, err = oitc.V4QueryPerfdata()
		if err != nil {
			logging.LogFatal("could not query database perfdata: ", err)
		}
	}

	if cli.sqlCache != "" {
		if data, err := json.Marshal(&perfdata); err != nil {
			logging.LogFatal("could not create json for sql cache file: %s", err)
		} else {
			if err := ioutil.WriteFile(cli.sqlCache, data, 0644); err != nil {
				logging.LogFatal("could not write sql cache file: %s", err)
			}
		}
	}

	logging.Log("created sql cache file")
	if cli.onlySQLCache {
		os.Exit(0)
	}

	return perfdata, nil
}

func main() {
	cli, err := parseCli()
	if err != nil {
		logging.LogFatal("%s", err)
	}

	if err = converter.SetRetention(cli.retention); err != nil {
		logging.LogFatal("%s", err)
	}

	lf, err := os.OpenFile(cli.logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logging.LogFatal("Could not open log file: %s", err)
	}
	defer lf.Close()
	log.SetOutput(lf)

	logging.Log("Version: %s", Version)

	// We have to use a seperate context for the workers, because they must be stopped
	// before the bars
	ctx, cancel := context.WithCancel(context.Background())
	workerCtx, workerCancel := context.WithCancel(ctx)
	var perfdata oitcdb.UUIDToPerfdata

	if cli.sqlCache != "" && cli.nosql {
		if data, err := ioutil.ReadFile(cli.sqlCache); err != nil {
			logging.LogFatal("could not read sql cache file: %s", err)
		} else {
			if err := json.Unmarshal(data, &perfdata); err != nil {
				logging.LogFatal("could not parse sql cache file: %s", err)
			}
		}
	}

	if !cli.nosql {
		perfdata, err = queryDB(ctx, cli)
	}

	logging.LogDisplay("Scanning %s for xml perfdata files", cli.sourceDirectory)
	var oldest time.Time
	if cli.maxAge > 0 {
		oldest = time.Now().Add(-time.Duration(cli.maxAge) * time.Second)
	}
	workdata, err := rrdpath.NewWorkdata(rrdpath.Walk(workerCtx, cli.sourceDirectory), oldest, cli.limit)
	if err != nil {
		logging.LogFatal("Could not scan rrd path: %s", err)
	}

	logging.LogDisplay(
		"Scanning finished\nTotal: %d Todo: %d After Limit: %d Too Old: %d Corrupt RRD: %d XML File Broken: %d",
		workdata.Total,
		workdata.Todo,
		len(workdata.RrdSets),
		workdata.TooOld,
		workdata.Corrupt,
		workdata.BrokenXML)
	if cli.checkOnly || len(workdata.RrdSets) == 0 {
		return
	}

	var wg sync.WaitGroup

	pb := mpb.NewWithContext(ctx, mpb.PopCompletedMode(), mpb.WithRefreshRate(1*time.Second))
	bar := pb.AddBar(
		int64(len(workdata.RrdSets)),
		mpb.BarNoPop(),
		mpb.PrependDecorators(decor.CountersNoUnit("%d / %d", decor.WCSyncWidth)),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
			decor.Elapsed(decor.ET_STYLE_GO, decor.WCSyncSpace),
			decor.NewAverageETA(decor.ET_STYLE_GO, time.Now(), decor.FixedIntervalTimeNormalizer(5), decor.WCSyncSpace),
		),
	)

	logging.PrintDisplayLog = func(message string) {
		pb.Add(0, makeLogBar(message)).SetTotal(0, true)
	}

	canSig := make(chan os.Signal, 1)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-canSig:
			workerCancel()
			logging.LogDisplayV("Waiting for running workers to finish")
			wg.Wait()
			logging.LogDisplayV("All workers stopped")
			bar.SetTotal(0, true)
			cancel()
			return
		}
	}()

	signal.Notify(canSig, os.Interrupt, os.Kill)

	cvt := &converter.Converter{Destination: cli.destDirectory, ArchivePath: cli.archiveDirectory, TempPath: cli.tempDirectory, Merge: !cli.noMerge, UUIDToPerfdata: perfdata, DeleteRRD: cli.deleteRRD}
	converter.NewWorker(workerCtx, &wg, workdata.RrdSets, cli.parallel, cvt, &barIncrementor{bar: bar})
	wg.Wait()
	pb.Wait()
}

func makeLogBar(msg string) mpb.FillerFunc {
	return func(w io.Writer, width int, st *decor.Statistics) {
		fmt.Fprint(w, msg)
	}
}

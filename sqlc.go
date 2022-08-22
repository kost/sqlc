package main;

const SQLC_VERSION string = "0.1.0"

import (
	"database/sql"
	_ "encoding/csv"
	"encoding/base64"
	"os"
	"os/exec"
	"fmt"
	"strings"
	"log"
	"runtime"
	"time"
	"math/rand"
	"github.com/chzyer/readline"
	"github.com/urfave/cli"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/uber/athenadriver/go"
	_ "github.com/nakagami/firebirdsql"
	_ "github.com/sijms/go-ora/v2"
	// external dependency on unixODBC
	// _ "github.com/alexbrainman/odbc"

)

func execmd(clis *cli.Context, cmd string) ([]byte) {
	shell:="/bin/sh"
	shellarg:="-c"
	if runtime.GOOS == "windows" {
		shell="cmd"
		shellarg="/c"
	}
	out, err := exec.Command(shell, shellarg, cmd).Output();
	if err != nil {
		if (clis.GlobalInt("debug")>1) { log.Printf("Error returned: %d\n",err); }
	}
	return out;
}

func encb64 (data []byte) (string) {
	senc := base64.StdEncoding.EncodeToString(data)
	return senc;
}

func encstrb64 (data string) (string) {
	senc := base64.StdEncoding.EncodeToString([]byte(data))
	return senc;
}

func DumpFlags(clis *cli.Context) {
	fmt.Printf("Database: %s\n", clis.GlobalString("database"))
	fmt.Printf("Connection: %s\n", clis.GlobalString("connection"))
	if len(clis.GlobalString("query")) > 0 {
		fmt.Printf("Query: %s\n", clis.GlobalString("query"))
	}
}

func dbdefquery(clis *cli.Context, db *sql.DB) {
	if len(clis.GlobalString("query")) > 0 {
		dbexe(clis,db,clis.GlobalString("query"))
	}
}


func CmdConsole(clis *cli.Context) error {
	fmt.Printf("Console test\n")

	DumpFlags(clis)

	db, _ := dbopen(clis)

	dbdefquery(clis,db)

	rl, err := readline.New("> ")
	if err != nil {
	    panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil { // io.EOF, readline.ErrInterrupt
			break
		}
		if (strings.EqualFold(line,"quit") || strings.EqualFold(line,"exit")) {
			break
		}
		if len(line)>0 {
			log.Printf("%s",line)
			dbexe(clis,db,line)
		}
	}
	return nil
}

func tableList(db *sql.DB, dbtype string, alltables bool) ([]string) {
	var (
		name string
	)
	var tablelist []string
	var query string

	// query := "SELECT * FROM pg_catalog.pg_tables"
	// SELECT table_name,table_type,table_schema FROM INFORMATION_SCHEMA.TABLES where table_schema='public'
	// SELECT * FROM INFORMATION_SCHEMA.TABLES

	query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema='public'"
	if dbtype == "mysql" && alltables {
		query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES"
	}
	if dbtype == "postgres" && alltables {
		query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES"
	}
	if dbtype == "sqlite3" {
		query = "SELECT name FROM sqlite_master WHERE type='table'"
	}

	rows, err := db.Query(query)

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		tablelist = append (tablelist, name)
	}

	total := uint64(0)
	for i, v := range tablelist {
		fmt.Printf("Table %d: %s\n", i, v)
		total=uint64(i)+uint64(1)
	}
	fmt.Printf("Found %d table(s)\n",total)
	return tablelist
}

func CmdList(clis *cli.Context) error {
	for _, name := range sql.Drivers() {
		fmt.Printf("%s\n", name)
	}
	return nil
}

func CmdDump(clis *cli.Context) error {
	var tablelist []string
	fmt.Printf("Dump test\n")


	DumpFlags(clis)

	// LIMIT 20 OFFSET
	offset := uint64(0)
	limit := uint64(2)

	db, err := dbopen(clis)

	if err != nil {
		log.Fatal(err)
	}

	if len(clis.String("table")) > 0 {
		tablelist = append (tablelist, clis.String("table"))
	} else {
		tablelist = tableList(db, clis.GlobalString("database"), clis.Bool("all-tables"))
	}

	rand.Seed(time.Now().Unix())
	sqlq := clis.GlobalString("query")
	if len(sqlq) > 0 {
	}
	for i, tablename := range tablelist {
		if (clis.GlobalInt("debug")>0) { log.Printf("Dumping table: %s (%d/%d)",tablename,i,i); }
		sqlq=fmt.Sprintf("SELECT * FROM %s", tablename)
		for  rowsret:=limit; rowsret == limit; offset=offset+rowsret {
			if clis.GlobalInt("max-rows") > 0 {
				maxrows := int64(clis.GlobalInt("max-rows"))
				minrows := int64(clis.GlobalInt("min-rows"))
				if minrows < 1 {
					minrows=1
				}
				randrows := int64(1)
				if minrows == maxrows {
					randrows = minrows
				} else {
					diffrows := maxrows-minrows
					if diffrows <= 0 {
						diffrows = 1
					}
					randrows = int64(minrows)+rand.Int63n(diffrows)
				}
				log.Printf("Fetching %d rows\n", randrows)
				limit=uint64(randrows)
			}
			sqlqe := fmt.Sprintf("%s LIMIT %d OFFSET %d",sqlq, limit, offset)
			if clis.GlobalString("database") == "mssql" {
				sqlqe = fmt.Sprintf("%s ORDER BY 1 OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", sqlq, offset, limit)
			}
			rowsret = dbexe(clis,db,sqlqe)
			if len(clis.GlobalString("delay-between")) > 0 {
				duration, _ := time.ParseDuration(clis.GlobalString("delay-between"))
				log.Printf("Sleeping for %f seconds\n", duration.Seconds())
				time.Sleep(duration)
			}
			if len(clis.GlobalString("max-delay")) > 0 {
				maxdelay_d, _ := time.ParseDuration(clis.GlobalString("max-delay"))
				mindelay_d, _ := time.ParseDuration(clis.GlobalString("min-delay"))
				maxdelay := maxdelay_d.Nanoseconds()
				mindelay := mindelay_d.Nanoseconds()
				if mindelay < 1 {
					mindelay = 1
				}
				randduration := time.Duration(int64(1))
				if mindelay == maxdelay {
					randduration = time.Duration(mindelay)
				} else {
					diffdelay := maxdelay - mindelay
					if diffdelay <= 0 {
						diffdelay = 1
						log.Printf("Delay error, have you forgot to use hms acronyms in delay or swapped min/max? \n")
					}
					randduration = time.Duration(mindelay+rand.Int63n(diffdelay))
				}
				log.Printf("Sleeping for %f seconds\n", randduration.Seconds())
				time.Sleep(randduration)
			}
		}
	}
	return nil
}

func dbopen(clis *cli.Context) (*sql.DB, error) {
	db,err := sql.Open(clis.GlobalString("database"), clis.GlobalString("connection"))
	if err != nil {
		log.Fatal(err)
	}
	errp := db.Ping()
	if errp != nil {
		log.Fatal(errp)
		return nil, errp
	}
	return db, err
}

func dbexe(clis *cli.Context, db *sql.DB, query string) (uint64) {
	var f *os.File

	fileopened := false
	rows, err := db.Query(query)
	if err != nil {
		log.Printf(err.Error())
		return uint64(0)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		log.Printf(err.Error())
		return uint64(0)
	}
	valsp := make([]interface{}, len(cols))
	rawResult := make([][]byte, len(cols))
	for i, _ := range rawResult {
		valsp[i] = &rawResult[i]
	}

	if len(clis.GlobalString("output")) > 0 {
		var erropen error
		f, erropen = os.OpenFile(clis.GlobalString("output"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if erropen != nil {
			fileopened = false
		} else {
			fileopened = true
			defer f.Close()
		}
	}

	rawheader:=""
	if clis.GlobalBool("printheader") {
		for _, v := range cols {
			vdef:=""
			if len(v) > 0 {
				vdef=string(v)
			} else {
				vdef="<NIL>"
			}
			columnname:=fmt.Sprintf("%s%s", vdef, clis.GlobalString("field"))
			fmt.Printf(columnname)
			rawheader=rawheader+columnname
		}
		fmt.Printf(clis.GlobalString("row"))
		rawheader=rawheader+clis.GlobalString("row")
		if fileopened {
			_, errwrite := f.WriteString(rawheader)
			if errwrite != nil {
				log.Printf("Error writting string: %s", errwrite.Error())
			}
		}
	}

	rowsret := uint64(0)
	if (clis.GlobalInt("debug")>18) { log.Printf("About to scan for next row"); }
	for rows.Next() {
		err = rows.Scan(valsp...)
		if err != nil {
			log.Printf(err.Error())
			return rowsret
		}
		rowdata:=""
		if (clis.GlobalInt("debug")>28) { log.Printf("Starting to loop rawResult"); }
		for i, r := range rawResult {
			if len(r) == 0 {
				fmt.Printf("<NIL>%s",clis.GlobalString("field"))
			} else {
				// fmt.Printf("%d: %s\t", i, string(r))
				cell4row:=fmt.Sprintf("%s%s",string(r), clis.GlobalString("field"))
				cellid:=fmt.Sprintf("%d_%d",rowsret,i)
				celldata:=fmt.Sprintf("%s",string(r))
				rowdata=rowdata+cell4row
				if len(clis.GlobalString("execute")) > 0 {
					cmd2exe:=clis.GlobalString("execute")
					strings.Replace(cmd2exe,"{DATA}",celldata,-1)
					strings.Replace(cmd2exe,"{B64DATA}",encb64(r),-1)
					strings.Replace(cmd2exe,"{CELLID}",cellid,-1)
					execmd(clis, cmd2exe)
				}
				fmt.Printf("%s%s", string(r), clis.GlobalString("field"))
				if (clis.GlobalInt("debug")>9) { log.Printf("Column number: %d, Value %s",i,string(r)); }
			}
		}
		fmt.Printf(clis.GlobalString("row"));
		if fileopened {
			n, errwrite := f.WriteString(rowdata)
			if errwrite != nil {
				log.Printf("Error writting string: %s", errwrite.Error())
			}
			n2, errwrite2 := f.WriteString(clis.GlobalString("row"))
			if errwrite2 != nil {
				log.Printf("Error writting string: %s", errwrite2.Error())
			}
			if (clis.GlobalInt("debug")>9) { log.Printf("Bytes written: %d Row delimiter: %d", n, n2); }
		}
		if len(clis.GlobalString("executerow")) > 0 {
			cmd2exe:=clis.GlobalString("executerow")
			strings.Replace(cmd2exe,"{B64ROWDATA}",encstrb64(rowdata),-1)
			strings.Replace(cmd2exe,"{ROWDATA}",rowdata,-1)
			strings.Replace(cmd2exe,"{ROWID}",string(rowsret),-1)
			execmd(clis, cmd2exe)
		}
		rowsret++
	}
	if (clis.GlobalInt("debug")>1) { log.Printf("Rows returned: %d\n",rowsret); }
	return rowsret
}

func main() {
	app := cli.NewApp()
	app.Name = "sqlc"
	app.Usage = "SQL Console Client"
	app.Version = SQLC_VERSION
	// global level flags
	app.Flags = []cli.Flag{
	    cli.IntFlag{
		Name:  "debug",
		Usage: "Show debugging output",
	    },
	    cli.BoolFlag{
		Name:  "verbose",
		Usage: "Show more output",
	    },
	    cli.BoolFlag{
		Name:  "printheader",
		EnvVar: "SQLC_PRINTHEADER",
		Usage: "Print header before query output",
	    },
	    cli.StringFlag{
		Name:  "d, driver, database",
		EnvVar: "SQLC_DRIVER",
		Value: "sqlite3",
		Usage: "Specify database driver to use",
	    },
	    cli.StringFlag{
		Name:  "c, connection",
		EnvVar: "SQLC_CON,SQLC_CONN",
		Usage: "Specify connection string to use (default: db.db)",
	    },
	    cli.StringFlag{
		Name:  "E, executerow",
		EnvVar: "SQLC_EXECUTE_ROW",
		Usage: "Specify command to execute for each row",
	    },
	    cli.StringFlag{
		Name:  "e, execute",
		EnvVar: "SQLC_EXECUTE_CELL",
		Usage: "Specify command to execute for each data inside column",
	    },
	    cli.StringFlag{
		Name:  "f, field",
		Value: "\t",
		EnvVar: "SQLC_FIELD_DELIMITER",
		Usage: "Specify field delimiter to use",
	    },
	    cli.StringFlag{
		Name:  "r, row",
		Value: "\n",
		EnvVar: "SQLC_ROW_DELIMITER",
		Usage: "Specify row delimiter to use",
	    },
	    cli.StringFlag{
		Name:  "q, query",
		EnvVar: "SQLC_QUERY",
		Usage: "Specify query to use (default: none)",
	    },
	    cli.StringFlag{
		Name:  "o, output",
		EnvVar: "SQLC_OUTPUT",
		Usage: "Specify output file (default: none)",
	    },
	    cli.StringFlag{
		Name:  "max-rows",
		EnvVar: "SQLC_MAX_ROWS",
		Usage: "Maximum rows returned per single query",
	    },
	    cli.StringFlag{
		Name:  "min-rows",
		EnvVar: "SQLC_MIN_ROWS",
		Usage: "Minimum rows returned per single query",
	    },
	    cli.StringFlag{
		Name:  "min-delay",
		EnvVar: "SQLC_MIN_DELAY",
		Usage: "Minimum Delay seconds between queries",
	    },
	    cli.StringFlag{
		Name:  "max-delay",
		EnvVar: "SQLC_MAX_DELAY",
		Usage: "Maximum Delay seconds between queries",
	    },
	    cli.StringFlag{
		Name:  "delay-between",
		EnvVar: "SQLC_DELAY_BETWEEN",
		Usage: "Delay seconds between queries",
	    },
	    cli.IntFlag{
		Name:  "limit-rows",
		EnvVar: "SQLC_LIMIT_ROWS",
		Usage: "Limit number of rows returned per query",
	    },
	}

	// Commands
	app.Commands = []cli.Command{
	    {
		Name: "console",
		Flags: []cli.Flag{
		    cli.BoolFlag{
			Name:  "save-history",
			Usage: "Save history in console",
		    },
		},
		Usage:  "Start in interactive console mode",
		Action: CmdConsole,
	    },
	    {
		Name: "list",
		Usage:  "List database drivers",
		Action: CmdList,
	    },
	    {
		Name: "dump",
		Flags: []cli.Flag{
		    cli.StringFlag{
			Name:  "table",
			EnvVar: "SQLC_TABLE",
			Usage: "Name of table to dump",
		    },
		    cli.BoolFlag{
			Name:  "all-tables",
			EnvVar: "SQLC_ALL_TABLES",
			Usage: "Dump all tables (including system tables)",
		    },
		},
		Usage:  "Dump data from database",
		Action: CmdDump,
	    },
	}


	app.Action = func(c *cli.Context) error {
		println("sqlc - Universal SQL client")
		DumpFlags(c)
		db, _ := dbopen(c)
		dbdefquery(c,db)
		return nil
	}

	sqlc_command := os.Getenv("SQLC_COMMAND")
	if len(sqlc_command) > 0 {
		cmdArgs := []string { os.Args[0], sqlc_command }
		app.Run(cmdArgs)
	} else {
		app.Run(os.Args)
	}
}


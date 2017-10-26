package main;

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
	"github.com/codegangsta/cli"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/denisenkom/go-mssqldb"
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
	    log.Printf("%s",line)
		if (strings.EqualFold(line,"quit") || strings.EqualFold(line,"exit")) {
			break
		}
	    dbexe(clis,db,line)
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
				diffdelay := maxdelay - mindelay
				if diffdelay <= 0 {
					diffdelay = 1
					log.Printf("Delay error, have you forgot to use hms acronyms in delay or swapped min/max? \n")
				}
				randduration := time.Duration(mindelay+rand.Int63n(diffdelay))
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
	vals := make([]interface{}, len(cols))
	rawResult := make([][]byte, len(cols))

	for i, v := range cols {
		vals[i] = &rawResult[i]
		fmt.Printf("%s%s", string(v), clis.GlobalString("field"))
	}
	fmt.Printf(clis.GlobalString("row"));

	rowsret := uint64(0)
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			log.Printf(err.Error())
			return rowsret
		}
		rowdata:=""
		for i, r := range rawResult {
			if r == nil {
				fmt.Printf("<NIL>\t")
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
	app.Version = "0.0.1"
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


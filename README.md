[![Circle Status](https://circleci.com/gh/kost/sqlc.svg?style=shield&circle-token=:circle-token)](https://circleci.com/gh/kost/sqlc)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c1ad8ed944fc4289ad6d4f5c3537f3ba)](https://www.codacy.com/manual/kost/sqlc?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=kost/sqlc&amp;utm_campaign=Badge_Grade)

# sqlc
sqlc - Universal DBMS/SQL client with exfiltration features :)

SQLc is universal and portable DBMS client. Although, it can be used as such, it have some universal features in order to help penetration testers. Imaging doing heavy exploitation behind perimeter and getting access to database, but still having hard time silently exfiltrating data from database? sqlc comes to rescue as single fat client for most popular databases! 
Also, it comes with different database exfiltration methods, so you can finally test your database firewall.

## installation

Standard go way on github:

```
git clone https://github.com/kost/sqlc
cd sqlc
go get
go build
```

## usage

### listing database drivers

```
$ ./sqlc list
awsathena
firebirdsql
firebirdsql_createdb
mssql
mysql
oracle
postgres
sqlite3
sqlmock
sqlserver
```

### console

```
./sqlc console
```

### dumping

```
./sqlc dump
```

### execute single query

```
./sqlc -q 'SELECT * FROM users'
```

## Examples

### PostgreSQL

```
./sqlc -d postgres -c "user=db password=db database=db host=172.17.0.2 sslmode=disable" console
```

### MySQL dump

Note: when SQLC_COMMAND is specified, all command line options are ignored. Due to stealthy dump...
```
export SQLC_CONN=dbuser:dbpasswd@tcp\(127.0.0.1\)/dbname
export SQLC_DRIVER=mysql
export SQLC_MAX_ROWS=100
export SQLC_MIN_ROWS=1000
export SQLC_MIN_DELAY=15s
export SQLC_MAX_DELAY=45s
export SQLC_TABLE=users
export SQLC_COMMAND=dump
./sqlc smtp trivial rewrite --all-options-ignored
```

### Sybase

Sybase ASE 12.5 or higher

```
tds://my_user:my_password@dbhost.com:5000/pubs?charset=utf8
```

### Oracle

Oracle simple:
```
oracle://user:pass@server/service_name
```

Oracle example with multiple servers
```
oracle://user:pass@server1/service?server=server2&server=server3
```



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
./sqlc list
mssql
mysql
postgres
sqlite3
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


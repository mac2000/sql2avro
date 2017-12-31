# SQL2AVRO

sql2avro is a command line tool to export query results to avro file

You gonna need it only if you are going to export big amounts of data. Otherwise consider use PowerShell.

This tool is used in conjunction with `bq load` to import data into BigQuery, e.g.:

```
sql2csv.exe --query="SELECT NotebookID, Name, Surname, convert(varchar, AddDate, 120), HeadquarterCityID AS CityID FROM NotebookEmployee with (nolock)" --output=notebookemployee.avro
bq load --replace db.notebookemployee notebookemployee.avro
```

## AVRO

Types are converted as is, so there might be cases when you will be forced to cast data to supported formats, otherwise app will die with error complaing that type is not supported, e.g.: if you ever get `Unhandled Exception: Avro.SchemaParseException: Undefined name: short` try to modify your query to something like this: `select cast(id as int) as id from city`. Also do not forget to give column names otherwise app will complain that if wont gues it, e.g.: if you ever see something like `Unhandled Exception: Avro.SchemaParseException: No "name" JSON field` it means that you probably forget add `as id` to given column

## Usage example:

```
sql2avro.exe --query="SELECT * FROM NotebookEmployee with (nolock)" --output=notebookemployee.avro --password=secret123
```

## Required options

`--query="select 1"` or `--input=query.sql` - provide query you wish to export

`--output=data.csv` - file to save results to

## Optional

`--server=localhost` - servername to connect to

`--database=RabotaUA2` - database to run query against

`--username=sa` - username

`--password=secret123` - password

# Build

Just clone repository, open solution in visual studio and build it


# CSVql - Nim
Query You'r CSVs data like a boss.

```bash
  Options(opt-arg sep :|=|spc):
  -h, --help                               write this help to stdout
  -q=, --query=       string     REQUIRED  Placing the query as an ANSI-SQL the table name should be replaced with the path
  to the csv instead
  i.e:
  1) SELECT name, day, hour FROM '/path/to/csv/test.csv' as
  t1 LIMIT 10
  2) SELECT name, lastname, birthday FROM '/path/to/csv/test.csv' as
  t1 LEFT JOIN '/path/to/csv/test2.csv' as t2 ON t1.name = t2.name
```

* Delimiter are gussed so no need to specified.

Query over CSV with simple AnsiSQL.

<kbd>
  <img src="https://rawgit.com/Bennyelg/csvql/master/demo.svg">
</kbd>

# Limitations:

## <i> 1. Header must be present. </i>
  If you'r header is with spaces / special chars they will be replaced.
  </br>
  Space will replace to (dash `_`) and special chars will be removed.
  </br>
  ``#`` char will be written as no (# of drivers -> no_of_drivers) 

## <i>2. Delimiter is guessed in app so the delimiters are limited to the following:</i>
```nim
const possibleDelimiters = @[";", ",", ":", "~", "*", "$", "#", "@", "/", "%", "^", "\t"]
```
   Please make sure its one of the following delimiters.

## <i> 3. Fit to memory. </i>
   As you may already know - sqlite is used to store your data in-memory. so the CSV/s should be fit.

## <i>4. SQLite should be installed.</i>


# TODOs:
1. Tests.

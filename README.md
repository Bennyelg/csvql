
<p align="center">
  <img width="460" height="200" src="https://github.com/Bennyelg/csvql/blob/master/logo.png">
</p>

# CSVql - Nim.
----

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

<p align="center">
  <img width="1800" height="800" src="https://github.com/Bennyelg/csvql/blob/master/csvql_2_0.png">
</p>


# TODOs:
1. Tests.
2. Simple documentation with more examples and limitations.


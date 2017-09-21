<p align="center">
  <img width="460" height="300" src="https://github.com/Bennyelg/csvql/blob/master/logo.png">
</p>
# CSVql - Nim.
----

Query over CSV with simple AnsiSQL.

Examples:
---

```bash
./csvql -sql="SELECT * FROM '/Users/benny/Downloads/Tel_Aviv_Benny_Net_Actions.csv' LIMIT 10" -H
```
```csv
Token,weekDayHour,state,low,medium,peak,peakHigh,Fire,ModelID
151d39,100,0,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,100,1,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,100,2,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,100,3,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,101,0,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,101,1,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,101,2,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,101,3,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,102,0,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
151d39,102,1,0.25,0.25,0.25,0.2,0.05,2017-08-27MODULE_A
```

```bash
./csvql -sql="SELECT avg(weekDayHour) FROM '/Users/benny/Downloads/Tel_Aviv_Benny_Net_Actions.csv' LIMIT 10" -H
```
```csv
avg(weekDayHour)
411.5
```
```bash
./csvql -sql="SELECT count(*) FROM '/Users/benny/Downloads/Tel_Aviv_Benny_Net_Actions.csv' LIMIT 10" -H
```
```csv
count(*)
168672
```




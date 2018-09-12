import 
  os, 
  parsecsv, 
  streams, 
  sequtils, 
  strutils, 
  db_sqlite,
  strformat,
  re,
  times,
  locks,
  algorithm,
  cligen,
  terminal


type
  Csv = ref object
    path:       string
    alias:      string
    hasHeader:  bool
    columns:    seq[string]
    types:      seq[string]
    delimiter:  char

  Database = object
    connection: DbConn


template guessType(s: string, t: untyped): bool = 
  var result = false
  try:
    discard t(s)
    result = true
  except Exception:
    discard
  result
  
proc guessCsvDelimiter(filePath: string): char =
  const maxSampleCountOfRows = 10
  const possibleDelimiters = @[";", ",", ":", "~", "*", "$", "#", "@", "/", "%", "^", "\t"]
  var fs = newFileStream(filePath, fmRead)
  var line = ""
  var rowNO = 0
  var results: seq[tuple[rowNo: int, deli: string, count: int]] = @[]
  while fs.readLine(line):
    if rowNO < maxSampleCountOfRows:
      for delimiter in possibleDelimiters:
        results.add((rowNO, delimiter, line.split(delimiter).len))
      inc rowNO
    else:
      break
  var highestCount = 0
  var mostPossibleDelimiter: seq[char] = @[]
  for delimiter in possibleDelimiters:
    let resultSetForTheCurrentDelimiter = results.filterIt(it.deli == delimiter)
    for row in resultSetForTheCurrentDelimiter:
      highestCount = max(results.mapIt(it.count))
    
    for row in resultSetForTheCurrentDelimiter:
      if row.count == highestCount:
        mostPossibleDelimiter.add(row.deli)
  
  var mostPossibleDelimiterClean = deduplicate(mostPossibleDelimiter)
  if mostPossibleDelimiterClean.len == 1:
    return mostPossibleDelimiter[0]
  elif mostPossibleDelimiterClean.len > 1:
    var highestCntDelimiter = 0
    var highestPossibleDelimiterPosition = 0
    for idx, d in mostPossibleDelimiterClean:
      var countOfPossibleDelimiter = count(mostPossibleDelimiterClean, d)
      if highestCntDelimiter < countOfPossibleDelimiter:
        highestCntDelimiter = countOfPossibleDelimiter
        highestPossibleDelimiterPosition = idx
    return  mostPossibleDelimiterClean[highestPossibleDelimiterPosition]
  else:
    return ','
  
proc appendCsv* (path, alias: string, hasHeader: bool = false): Csv  =
  ## return a new Csv object.
  
  Csv(
    path: path,
    alias: alias,
    hasHeader: hasHeader,
    delimiter: guessCsvDelimiter(path)  # the default until we find otherwise.
  )

proc getTypesWithMostProbability(types: seq[seq[string]]): seq[string] =
  var totalTimeIsThere = 0
  var pos = 0
  for idx, row in types:
    var rowRecurrentCount = count(types, row)
    if totalTimeIsThere < rowRecurrentCount:
      pos = idx
      totalTimeIsThere = rowRecurrentCount

  types[pos]

proc figureColumnsTypes(rowsSamples: seq[seq[string]]): seq[string] =
  var types: seq[seq[string]] = @[]
  for row in rowsSamples:
    var rowTypes: seq[string] = @[]
    for item in row:
      if guessType(item, parseInt):
        rowTypes.add("int")
      elif guessType(item, parseFloat):
        rowTypes.add("float")
      else:
        rowTypes.add("string")
    types.add(rowTypes)

  let csvTypes = getTypesWithMostProbability(types)
  return csvTypes

proc parseCsv* (csv: Csv) =
  const numOfSamplingRows = 50
  var csvStream = newFileStream(csv.path, fmRead)
  var parser: CsvParser
  var rowsSampleCount: seq[seq[string]]
  parser.open(csvStream, csv.path, csv.delimiter)
  
  if csv.hasHeader:
    parser.readHeaderRow()
    csv.columns = parser.headers.mapIt(
      it.replace(" ", "_")
        .replace("'","")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "")
        .replace("#", "no")
        .toLowerAscii())
  var samplingCounter = 0
  while parser.readRow():
    let row = parser.row
    if numOfSamplingRows > samplingCounter:
      rowsSampleCount.add(row)
      inc samplingCounter
    else:
      break
  
  if not csv.hasHeader:
    var columns: seq[string] = @[]
    for idx in countup(1, rowsSampleCount[0].len):
      columns.add(fmt"c_{idx}")
    csv.columns = columns
  
  csv.types = figureColumnsTypes(rowsSampleCount)

proc openConnection* (): Database =
  Database(
    connection:  open(":memory:", nil, nil, nil)
  )

proc createTableUsingCsvProperties(db: Database, csv: Csv) =
  var columnsWithTypes: seq[string] = @[]
  for idx, column in csv.columns:
    columnsWithTypes.add(fmt"{column} {csv.types[idx]}")
  
  var statement = fmt"""
    CREATE TABLE {csv.alias} (
      {columnsWithTypes.join(",")}
    );
  """
  db.connection.exec(SqlQuery(statement))

proc executeChunk(args: tuple[db: Database, tableName: string, columns: seq[string], rows: seq[seq[string]]]) =
  var statement = fmt"""
    INSERT INTO {args.tableName}({args.columns.join(",")}) 
    VALUES
    """
  var insertableRows: seq[string] = newSeqOfCap[string](args.rows.len)
  for row in args.rows:
    var insertableRow = "(" & row.mapIt("'" & it.replace("?", "").replace("'","") & "'").join(",") & ")"
    insertableRows.add(insertableRow)
  let executableStatement = statement & insertableRows.join(",") & ";"  
  discard args.db.connection.tryExec(SqlQuery(executableStatement))

proc insertCsvRowsIntoTable(db: Database, csv: Csv) =
  db.connection.exec(sql"PRAGMA synchronous=OFF")
  db.connection.exec(sql"BEGIN TRANSACTION;")

  const defaultChunkSize = 1
  var rowsChunk: seq[seq[string]] = @[]
  var csvStream = newFileStream(csv.path, fmRead)
  var parser: CsvParser
  parser.open(csvStream, csv.path, csv.delimiter)
  if csv.hasHeader:
    parser.readHeaderRow()
  while parser.readRow():
    rowsChunk.add(parser.row)
    if rowsChunk.len == defaultChunkSize:
      executeChunk((db: db, tableName: csv.alias, columns: csv.columns, rows: rowsChunk))
      rowsChunk = @[]
  if rowsChunk.len > 0:
    executeChunk((db: db, tableName: csv.alias, columns: csv.columns, rows: rowsChunk))

  db.connection.exec(sql"COMMIT;") 

proc `*`(size: int, del: string): string =
  result = "+" 
  for i in countup(0, size):
    result &= del
  return result & "+"

proc getMaxLengthRow(rs: seq[tuple[r: Row, length: int]]): int =
  var position = 0
  var currentMaxSize = 0
  for idx, rx in rs:
    if rx.r.join("|").len > currentMaxSize:
      currentMaxSize = rx.r.len
      position = idx 
  return position

proc getQueryColumns(csvs: seq[Csv], query: string): seq[string] =
  let columnsRequested = query.toLowerAscii().split("from")[0].replace("select", "").strip().split(",")
  var columns: seq[string] = @[]
  if columnsRequested[0] == "*":
    for csv in csvs:
      columns.add(csv.columns.join(","))
      return deduplicate(columns).join(",").split(",").mapIt(it.strip())
  else:
    for t in columnsRequested:
      if ".*" in t:
        var tableRequestedPosition = parseInt(t.strip().split(".*")[0].replace("t", "")) - 1
        var tableName = t.strip().split(".*")[0]
        columns.add(csvs[tableRequestedPosition].columns.mapIt(tableName & "." & it.strip()))
      else:
        columns.add(t)

    return columns

  return columnsRequested.mapIt(it.strip())

proc getLongestWordsByPosition(rs: seq[tuple[r: Row, length: int]]): seq[int] =
  var lengths: seq[int] = newSeq[int](rs[0].r.len)
  for row in rs:
    for idx, word in row.r:
      if word.len > lengths[idx]:
        lengths[idx] = word.len
  return lengths

proc exportResults(columns: seq[string], resultSet: seq[seq[string]]): string =
  let dt = format(now(), "yyyy-mm-ddHH:mm:ss").replace("-","_").replace(":", "_")
  let generatedFilePath = getTempDir() & dt & ".csv"
  var fs = newFileStream(generatedFilePath, fmWrite)
  for idx, row in resultSet:
    if idx == 0:
      fs.writeLine(columns.join(","))
    fs.writeLine(row.join(","))
  
  return generatedFilePath

proc displayResults(db: Database, csvs: seq[Csv], query: string, exportResult: bool = false) =
  var queryColumns = getQueryColumns(csvs, query)
  var rows: seq[tuple[r: Row, length: int]] = @[]  
  for row in db.connection.fastRows(SqlQuery(query)):
    rows.add((
              r: row, 
              length: ("|" & row.join(",")).len
              )
            )
  var maxLengthPos = getMaxLengthRow(rows)
  var maxLengthOfWordsByPosition = getLongestWordsByPosition(rows)
  var fin: seq[seq[string]] = @[]
  var columns: seq[string] = @[]
  for row in rows:
    var words: seq[string] = @[]
    for idx, word in row.r:
      if maxLengthOfWordsByPosition[idx] > queryColumns[idx].len:
        words.add(center(word, maxLengthOfWordsByPosition[idx]))
      else:
        words.add(center(word, queryColumns[idx].len))
    fin.add(words)
  for idx, column in queryColumns:
    columns.add(center(column, maxLengthOfWordsByPosition[idx]))
  echo(fin[0].join("|").len * "-")
  echo("|" & columns.join("|") & " |")
  echo(fin[0].join("|").len * "-")
  for row in fin:
    echo("|" & row.join("|") & " |")
    echo(row.join("|").len * "-")
  
  if exportResult:
    let exportResultHeader = """
----------
::Export::
----------
"""
    styledWriteLine(stdout, fgRed, exportResultHeader, resetStyle)
    let generatedCsvPath = exportResults(queryColumns, rows.mapIt(it.r))
    styledWriteLine(stdout, fgGreen, "File is ready & can be located in: " & generatedCsvPath, resetStyle)

proc parseQuery(query: string): (seq[Csv], string) =
  let csvsPaths = re.findAll(query, re"'(.*?).csv'")
  var csvs = newSeqOfCap[Csv](csvsPaths.len)
  var newQuery = query
  let propertiesHeader ="""
----------------------
::Parsing Properties::
----------------------"""
  styledWriteLine(stdout, fgRed, propertiesHeader, resetStyle)

  for idx, csvPath in csvsPaths:
    styledWriteLine(stdout, fgGreen, fmt"t{idx + 1} = {csvPath}", resetStyle)
    let csv = appendCsv(csvPath.replace("'", ""), fmt"t{idx + 1}", true)
    csvs.add(csv)
    newQuery = newQuery.replace(csvPath.replace("'", ""), fmt"t{idx + 1}")
    

  return (csvs, newQuery)
  
proc csvQL(query: string, exportResult: bool = false) =
  let startTime = cpuTime()
  let db = openConnection()
  let (csvs, adjustedQuery) = parseQuery(query)
  let generatedQueryHeader = """
-------------------
::Generated Query::
-------------------"""
  styledWriteLine(stdout, fgRed, generatedQueryHeader, resetStyle)
  styledWriteLine(stdout, fgGreen, adjustedQuery, resetStyle)

  for csv in csvs:
    parseCsv(csv)
    db.createTableUsingCsvProperties(csv)
    db.insertCsvRowsIntoTable(csv)
  let queryResultHeader = """
----------
::Result::
----------"""
  styledWriteLine(stdout, fgRed, queryResultHeader, resetStyle)
  displayResults(db, csvs, adjustedQuery, exportResult)
  styledWriteLine(stdout, fgYellow, fmt"* Total Duration: {cpuTime() - startTime} seconds.", resetStyle)

when isMainModule:
  dispatch(csvQL, help = { "query" : 
"""Placing the query as an ANSI-SQL the table name should be replaced with the path to the csv instead
i.e:
1) SELECT name, day, hour FROM '/path/to/csv/test.csv' as t1 LIMIT 10 
2) SELECT name, lastname, birthday FROM '/path/to/csv/test.csv' as t1 LEFT JOIN '/path/to/csv/test2.csv' as t2 ON t1.name = t2.name
""", "exportResult": "set to true if you want to export the result set." })



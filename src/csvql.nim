from nre import findAll, re
import db_sqlite, strutils, strscans,
       sequtils, streams, typetraits,
       parsecsv, tables, os, parseopt2,
       times
       
const InsertChunkSize = 20_000

proc parseCSVTypes*(row: seq[string], optionalHeader: seq[string] = @[]): OrderedTable[string, string] =
    var metaFileType = initOrderedTable[string, string]()
    var header: seq[string] = @[]
    if optionalHeader.len == 0:
       for x in countup(0, row.len):
           header.add(format("c_0$1", x))
    else:
        header = optionalHeader 
    for ind, el in row:
        try:
            var t = el.parseInt()
            metaFileType[header[ind]] = t.type.name
            continue
        except ValueError:
            discard
        try:
            var t = el.parseFloat()
            metaFileType[header[ind]] = t.type.name
            continue
        except ValueError:
            discard
        metaFileType[header[ind]] = "text"
    return metaFileType

proc generateCreateStatement*(metaFileData: OrderedTable): string =
    var createStatement = """
        CREATE TABLE tmpTable(
    """
    for k, v in metaFileData:
        createStatement = createStatement & format("$1 $2,", k, v) & "\n"
    createStatement = createStatement[0..createStatement.len-3] & ");"
    return createStatement

proc createTable(db: DbConn, createStatment: string) =
    try:
        db.exec(SqlQuery(createStatment))
    except DbError as err:
        quit(err.msg)

proc insertStatement*(row: seq[string]): string =
    var rowValues = ""
    for val in row:
        rowValues = rowValues & ", " & format("'$1'", val) & " "
    rowValues = "(" & rowValues & ")"
    rowValues = rowValues.replace("(,", "(")
    return rowValues

proc writeHelp() =
    echo("""Usage csvql:
            -sql= or --sql=     The sql Statement instad of table provide the full file path.
            -h or -H            If your csv has header.
        
            e.g execution:
                -sql="SELECT * FROM '/path/to/file.csv' .." -H
    """)

proc fetchQueryHeader(sqlStatement: string, headerColumns: seq[string]): string =
    var selectColumns = nre.findAll(sqlStatement, re"(?<=SELECT)(.*)(?=FROM)")[0].strip().split(",")
    if selectColumns.len == 1 and selectColumns[0] == "*":
        return headerColumns.join(",")
    return selectColumns.join(",")

proc writeVersion() =
    echo("Version 1.0")

proc parseArguments(): Table[string, string] =
    var userArguments = initTable[string, string]()
    for kind, key, value in getopt():
        case kind
        of cmdLongOption,cmdShortOption:
            if key.contains("sql"):
                userArguments["sql"] = value
            if key.toLowerAscii().contains("header") or key == "H":
                userArguments["header"] = "true"
            if key == "d" or key.toLowerAscii.contains("delimiter"):
                userArguments["delimiter"] = value
        of cmdArgument: 
            if key == "help": 
                writeHelp()
                quit()
            if key == "version":
                writeVersion()
                quit()
        of cmdEnd: discard
    if not userArguments.len != 1 and not userArguments.hasKey("sql"):
        writeHelp()
        quit()
    return userArguments

proc analyzeLimit(statement: string): int =
    result = -1
    let limitClause = statement.toLowerAscii().contains("limit")
    let whereClause = statement.toLowerAscii().contains("where")
    let aggFunctions = statement.toLowerAscii().contains("group by")
    try:
        # If we have a where clause we cannot take the limit before, so we have to ignore.
        if limitClause and not whereClause and not aggFunctions:
            result = statement.toLowerAscii().split("limit")[1].strip().parseInt()
    except ValueError:
        discard
    return result

proc processCSVData(db: DbConn, args: var Table[string, string]): Table[string, string] =
    var 
        i = 0
        parser: CsvParser
        fullPath = nre.findAll(args["sql"], re"\'(.*)\'")
        filePath = fullPath[0].replace("'", "")
        deli = ','
        valuesHolder: seq[string] = @[]
        csvHeader: seq[string]
        currentPosition = 0
    args["sql"] = args["sql"].replace(fullPath[0], "tmpTable")
    let limit = analyzeLimit(args["sql"])
    var fileStream = newFileStream(filePath, fmRead)
    
    if fileStream == nil:
        quit("file not found.")

    if args.hasKey("delimiter"):
        deli = args["delimiter"][0]

    parser.open(fileStream, filePath, deli)
    
    if args.hasKey("header"):
        parser.readHeaderRow()
        csvHeader = parser.headers


    while parser.readRow():
        if i == 0:
            var typeMapping = parseCSVTypes(parser.row, csvHeader)
            args["query_header"] = fetchQueryHeader(args["sql"], toSeq(typeMapping.keys()))
            let statement = generateCreateStatement(typeMapping)
            createTable(db, statement)
        valuesHolder.add(insertStatement(parser.row))
        i.inc
        currentPosition.inc
        if i == limit and limit != -1:
            break
        if InsertChunkSize == currentPosition:
            let insertStatement = "INSERT INTO tmpTable VALUES " & valuesHolder.join(",")
            db.exec(SqlQuery(insertStatement))
            currentPosition = 0
            valuesHolder = @[]
    # Dumping the rest.
    let insertStatement = "INSERT INTO tmpTable VALUES " & valuesHolder.join(",")
    db.exec(SqlQuery(insertStatement))
    return args

proc executeUserQuery(db: DbConn, userParsedArguments: Table[string, string]) =
    var prettyHeader = userParsedArguments["query_header"].strip().split(",").mapIt(string, it.strip()).join(",")
    echo(prettyHeader)
    for r in db.fastRows(SqlQuery(userParsedArguments["sql"])):
        echo(r.join(","))
    discard

when isMainModule:
    let db = db_sqlite.open(":memory:", nil, nil, nil)
    var args = parseArguments()
    var startTime = cpuTime()
    var readyArguments = processCSVData(db, args)
    var endTime = cpuTime()
    echo("# Statistics: =>")
    echo("# Total time spending inserting rows: ", (endTime - startTime))
    executeUserQuery(db, readyArguments)
    var executionEndTime = cpuTime()
    echo("# Total query execution spending time: ", (executionEndTime - endTime))

    
import db_sqlite
import strutils
import sequtils
import streams
import typetraits
import parsecsv
import tables
import os
import parseopt2
from nre import findAll, re
import strscans

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

proc createTable*(db: DbConn, createStatment: string): bool =
    try:
        db.exec(SqlQuery(createStatment))
    except DbError as err:
        quit(err.msg)
    return true

proc appendInsert*(row: seq[string]): string =
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
    echo("version 1.0")

proc parseArguments*(): Table[string, string] =
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

proc processCSVData*(db: DbConn, args: var Table[string, string]): Table[string, string] =
    var 
        i = 0
        p: CsvParser
        x = nre.findAll(args["sql"], re"\'(.*)\'")
        filePath = x[0].replace("'", "")
        deli = ","
        valuesHolder: seq[string] = @[]
        csvHeader: seq[string]
    args["sql"] = args["sql"].replace(x[0], "tmpTable")
    var s = newFileStream(filePath, fmRead)
    if s == nil:
        quit("file not found.")

    p.open(s, filePath)
    
    if args.hasKey("header"):
        p.readHeaderRow()
        csvHeader = p.headers
    if args.hasKey("delimiter"):
        deli = args["delimiter"]

    while p.readRow():
        if i == 0:
            var typeMapping = parseCSVTypes(p.row, csvHeader)
            args["query_header"] = fetchQueryHeader(args["sql"], toSeq(typeMapping.keys()))
            var statement = generateCreateStatement(typeMapping)
            discard createTable(db, statement)
        valuesHolder.add(appendInsert(p.row))
        i.inc
    var insertStatement = "INSERT INTO tmpTable VALUES " & valuesHolder.join(",")
    db.exec(SqlQuery(insertStatement))
    return args

proc executeUserQuery*(db: DbConn, userParsedArguments: Table[string, string]) =
    var prettyHeader = ""
    prettyHeader = userParsedArguments["query_header"].strip().split(",").mapIt(string, it.strip()).join(",")
    echo(prettyHeader)
    for r in db.fastRows(SqlQuery(userParsedArguments["sql"])):
        echo(r.join(","))
    discard

when isMainModule:
    let db = db_sqlite.open(":memory:", nil, nil, nil)
    var args = parseArguments()
    var readyArguments = processCSVData(db, args)
    executeUserQuery(db, readyArguments)
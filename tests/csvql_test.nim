import tables
import strutils
import unittest
import ../src/csvql

suite "Should return as we expected by giving values.":
    let input = @["151d39","100","0","0.25","0.25","0.25","0.2","0.05","2017-08-27MODULE_A"]
    let expectedOutput = [("c_00", "text"),
                            ("c_01", "int"),
                            ("c_02", "int"),
                            ("c_03", "float"),
                            ("c_04", "float"),
                            ("c_05", "float"),
                            ("c_06", "float"),
                            ("c_07", "float"),
                            ("c_08", "text")
                        ].toOrderedTable
    let expectedOutputWithHeader = [
                            ("a", "text"),
                            ("b", "int"),
                            ("c", "int"),
                            ("d", "float"),
                            ("e", "float"),
                            ("f", "float"),
                            ("g", "float"),
                            ("h", "float"),
                            ("i", "text")
                        ].toOrderedTable
    
    test "parsed row should be equal to the expected.":
        check(parseCSVTypes(input) == expectedOutput)
    
    test "when specified header it should return with the header":
        let header = @["a", "b", "c", "d", "e", "f", "g", "h", "i"]
        check(parseCSVTypes(input, header) == expectedOutputWithHeader)

    test "when provide the orderedTable it should create table statement apropiratly":
        var expectedCreateTable = """
                    CREATE TABLE tmpTable(
                    a text,
                    b int,
                    c int,
                    d float,
                    e float,
                    f float,
                    g float,
                    h float,
                    i text);""".unindent
        check(generateCreateStatement(expectedOutputWithHeader).unindent == expectedCreateTable)

    test "by giving row as a seq it should translate it to an tuple like format":
        var expectedTuple = "( '151d39' , '100' , '0' , '0.25' , '0.25' , '0.25' , '0.2' , '0.05' , '2017-08-27MODULE_A' )"
        check(insertStatement(input) == expectedTuple)
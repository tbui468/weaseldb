package main

import (
    "wsldb"
//    "io/ioutil"
//    "os"
//    "bufio"
    "fmt"
)

func PrintResult(readers []wsldb.Reader) {
    for _, reader := range readers {
        for row := 0; row < reader.RowCount; row++ {
            for col := 0; col < reader.ColCount; col ++ {
                switch(wsldb.NextType(&reader)) {
                case wsldb.Int8:
                    d := wsldb.NextInt8(&reader)
                    fmt.Printf("%d,", d)
                case wsldb.Float4:
                    f := wsldb.NextFloat4(&reader)
                    fmt.Printf("%.1f,", f)
                case wsldb.Text:
                    s := wsldb.NextText(&reader)
                    fmt.Printf("%s,", s)
                case wsldb.Bytea:
                    b := wsldb.NextBytea(&reader)
                    fmt.Printf("\\x%s,", fmt.Sprintf("%x", b))
                case wsldb.Bool:
                    b := wsldb.NextBool(&reader)
                    if b {
                        fmt.Printf("true,")
                    } else {
                        fmt.Printf("false,")
                    }
                case wsldb.Null:
                    fmt.Printf("null,")
                default:
                    print("Invalid datum type")
                }
            }
            fmt.Printf("\n")
        }
    }
}


func main() {
    conn1 := wsldb.ConnectToServer("localhost:3000")
    defer conn1.Close()

    //conn2 := wsldb.ConnectToServer("localhost:3000")
    //defer conn2.Close()

    PrintResult(wsldb.ExecuteQuery(conn1, "create table planets (name text, moons int8);"))
    PrintResult(wsldb.ExecuteQuery(conn1, "insert into planets (name, moons) values ('Earth', 1);"))
//    PrintResult(wsldb.ExecuteQuery(conn1, "begin;"))
    PrintResult(wsldb.ExecuteQuery(conn1, "begin; insert into planets (name, moons) values ('Mars', 2); commit;"))
    //PrintResult(wsldb.ExecuteQuery(conn2, "select name, moons from planets order by name asc;")) //should NOT see Mars, 2 since conn1 didn't commit yet
//    PrintResult(wsldb.ExecuteQuery(conn1, "commit;"))
    PrintResult(wsldb.ExecuteQuery(conn1, "drop table planets;"))
    //testing read-committed isolation - cannot overwrite non-committed write.  cannot read non-commited writes

/*
create table planets (name text, moons int8);

insert into planets (name, moons) values ('Earth', 1);
begin;
insert into planets (name, moons) values ('Mars', 2);
select name, moons from planets order by name asc;
commit;

drop table planets;*/
}

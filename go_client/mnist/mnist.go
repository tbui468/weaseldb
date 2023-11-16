package main

import (
    "wsldb"
    "encoding/binary"
    "strconv"
    "encoding/hex"
//    "io/ioutil"
    "os"
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
    conn := wsldb.ConnectToServer("localhost:3000")
    defer conn.Close()
    wsldb.ExecuteQuery(conn, "create table mnist (label int8, data bytea);")
    wsldb.ExecuteQuery(conn, "begin; create model my_model ('mnist_model.pt'); commit;")

    f1, _ := os.Open("test.labels")
    magic := make([]byte, 4)
    f1.Read(magic)
    count_bytes := make([]byte, 4)
    f1.Read(count_bytes)
    count := int(binary.BigEndian.Uint32(count_bytes))

    f2, _ := os.Open("test.images")
    f2.Read(magic)
    f2.Read(count_bytes)
    f2.Read(count_bytes) //pixels per row
    f2.Read(count_bytes) //pixels per col

    query := "insert into mnist (label, data) values "
    for i := 0; i < 10; i++ {
        label := make([]byte, 1)
        f1.Read(label)
        label_as_str := strconv.Itoa(int(label[0]))

        pixels := make([]byte, 28 * 28)
        f2.Read(pixels)
        pixels_as_str := hex.EncodeToString(pixels)

        query += "(" + label_as_str + ",  '\\x" + pixels_as_str + "')"

        if i != count - 1 {
            query += ", "
        }
    }

    query += ";"

    wsldb.ExecuteQuery(conn, query)
    //PrintResult(wsldb.ExecuteQuery(conn, "select label, data from mnist where _rowid < 10;"))
    PrintResult(wsldb.ExecuteQuery(conn, "begin; select label, my_model(data) from mnist where _rowid < 10; commit;"))
    //PrintResult(wsldb.ExecuteQuery(conn, "select label = my_model(data) from mnist where _rowid < 10;"))
    wsldb.ExecuteQuery(conn, "drop table mnist;")
}

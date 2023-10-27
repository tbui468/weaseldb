package main

import (
    "wsldb"
    "io/ioutil"
    "os"
    "bufio"
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

    args := os.Args[1:]

    switch len(args) {
    case 0:
        reader := bufio.NewReader(os.Stdin)
        for {
            print(">")
            text, err := reader.ReadString('\n')
            if err != nil {
                os.Exit(1)
            }
            PrintResult(wsldb.ExecuteQuery(conn, text))
        }
    case 1:
        bytes, err := ioutil.ReadFile(args[0])
        if err != nil {
            os.Exit(1)
        }

        PrintResult(wsldb.ExecuteQuery(conn, string(bytes)))
    default:
        println("Usage: go_client [filename]")
        os.Exit(1)
    }
}

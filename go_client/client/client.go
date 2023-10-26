package main

import (
    "wsldb"
    "os"
    "bufio"
    "io/ioutil"
)

func main() {
    args := os.Args[1:]

    conn := wsldb.ConnectToServer("localhost:3000")
    defer conn.Close()

    switch len(args) {
    case 0:
        reader := bufio.NewReader(os.Stdin)
        for {
            print(">")
            text, err := reader.ReadString('\n')
            if err != nil {
                os.Exit(1)
            }
            wsldb.ProcessQuery(conn, text)
        }
    case 1:
        bytes, err := ioutil.ReadFile(args[0])
        if err != nil {
            os.Exit(1)
        }

        wsldb.ProcessQuery(conn, string(bytes))
    default:
        println("Usage: go_client [filename]")
        os.Exit(1)
    }
}

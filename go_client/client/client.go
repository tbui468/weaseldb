package main

import (
    "wsql"
    "os"
//    "bufio"
    "io/ioutil"
    "github.com/gin-gonic/gin"
    "net/http"
    "fmt"
)

type Atm struct {
    Id          string  `json:"id"`
    Location    string  `json:"location"`
}

var atms = []Atm {
    {Id: "1", Location: "New York"},
    {Id: "2", Location: "Seattle"},
    {Id: "3", Location: "Los Angeles"},
}

func getAtms(c *gin.Context) {
    //TODO: query database here
    //Reader should be used to grab data (in the correct data type) from result
    c.IndentedJSON(http.StatusOK, atms)
}

func getAtmById(c *gin.Context) {
    id := c.Param("id")

    for _, a := range atms {
        if a.Id == id {
            c.IndentedJSON(http.StatusOK, a)
            return
        }
    }

    c.IndentedJSON(http.StatusNotFound, gin.H{"message": "atm not found"})
}

func postAtms(c *gin.Context) {
    var newAtm Atm
    
    if err := c.BindJSON(&newAtm); err != nil {
        return
    }

    atms = append(atms, newAtm)
    c.IndentedJSON(http.StatusCreated, newAtm)
}

func main() {
    conn := wsql.ConnectToServer("localhost:3000")
    defer conn.Close()

    seed, err := ioutil.ReadFile("seed.sql")
    if err != nil {
        os.Exit(1)
    }
    
    wsql.ExecuteQuery(conn, string(seed))

    readers := wsql.ExecuteQuery(conn, "select _rowid from atm_locations where _rowid = 111; select _rowid, bank_name, address from atm_locations where zip_code = 1000 or zip_code = 11011;")

    for _, reader := range readers {
        for row := 0; row < reader.RowCount; row++ {
            for col := 0; col < reader.ColCount; col ++ {
                switch(wsql.NextType(&reader)) {
                case wsql.Int8:
                    d := wsql.NextInt8(&reader)
                    fmt.Printf("%d,", d)
                case wsql.Float4:
                    f := wsql.NextFloat4(&reader)
                    fmt.Printf("%.1f,", f)
                case wsql.Text:
                    s := wsql.NextText(&reader)
                    fmt.Printf("%s,", s)
                case wsql.Bool:
                    b := wsql.NextBool(&reader)
                    if b {
                        fmt.Printf("true,")
                    } else {
                        fmt.Printf("false,")
                    }
                case wsql.Null:
                    fmt.Printf("null,")
                default:
                }
            }
            fmt.Printf("\n")
        }
    }

    router := gin.Default()
    router.GET("/atms", getAtms)
    router.GET("/atms/:id", getAtmById)
    router.POST("/atms", postAtms)
    router.Run("localhost:8080")

    /*
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
            wsql.ProcessQuery(conn, text)
        }
    case 1:
        bytes, err := ioutil.ReadFile(args[0])
        if err != nil {
            os.Exit(1)
        }

        wsql.ProcessQuery(conn, string(bytes))
    default:
        println("Usage: go_client [filename]")
        os.Exit(1)
    }*/
}
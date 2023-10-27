package main

import (
    "wsldb"
    "os"
    "net"
    "io/ioutil"
    "github.com/gin-gonic/gin"
    "net/http"
    "strconv"
)

type Atm struct {
    Id          int64   `json:"id"`
    BankName    string  `json:"bank_name"`
    Address     string  `json:"address"`
    Country     string  `json:"country"`
    City        string  `json:"city"`
    State       string  `json:"state"`
    ZipCode     int64   `json:"zip_code"`
}

func getAtms(c *gin.Context) {
    conn, ok := c.MustGet("tcpConn").(*net.TCPConn)
    if !ok {
        os.Exit(1);
    }

    readers := wsldb.ExecuteQuery(conn, "select _rowid, bank_name, address, country, city, state, zip_code from atm_locations;")
    atms := FillStruct(readers[0])
    c.IndentedJSON(http.StatusOK, atms)
}

func getAtmById(c *gin.Context) {
    conn, ok := c.MustGet("tcpConn").(*net.TCPConn)
    if !ok {
        os.Exit(1);
    }

    id := c.Param("id")

    //TODO: should not be concatenating values like this - big potential security issue with SQL injection
    readers := wsldb.ExecuteQuery(conn, "select _rowid, bank_name, address, country, city, state, zip_code from atm_locations where _rowid = " + string(id) + ";")

    if len(readers) == 0 {
        c.IndentedJSON(http.StatusNotFound, gin.H{"message": "atm not found"})
        return
    }

    atms := FillStruct(readers[0])
    c.IndentedJSON(http.StatusOK, atms)

}

func Quote(s string) string {
    return "'" + s + "'"
}

func postAtms(c *gin.Context) {
    conn, ok := c.MustGet("tcpConn").(*net.TCPConn)
    if !ok {
        os.Exit(1);
    }

    var a Atm
    if err := c.BindJSON(&a); err != nil {
        return
    }

    //TODO: should not be concatenating values like this - big potential security issue with SQL injection
    values := Quote(a.BankName) + ", " + Quote(a.Address) + ", " + Quote(a.Country) + ", "  + Quote(a.City) + ", " + Quote(a.State) + ", " + strconv.FormatInt(a.ZipCode, 10)
    insert := "insert into atm_locations (bank_name, address, country, city, state, zip_code) values (" + values + ");"
    query := "select max(_rowid) from atm_locations;"
    start := "begin;"
    end := "commit;"

    readers := wsldb.ExecuteQuery(conn, start + insert + query + end)
    for _, reader := range readers {
        if reader.RowCount != 0 {
            wsldb.NextType(&reader)
            a.Id = wsldb.NextInt8(&reader)
        }
    }

    c.IndentedJSON(http.StatusCreated, a)
}

func FillStruct(reader wsldb.Reader) []Atm {
    atms := make([]Atm, 0)

    for row := 0; row < reader.RowCount; row++ {
        atm := Atm{Id: 0, BankName: "", Address: "", Country: "", City: "", State: "", ZipCode: 0}

        wsldb.NextType(&reader)
        atm.Id = wsldb.NextInt8(&reader);

        if wsldb.NextType(&reader) != wsldb.Null {
            atm.BankName = wsldb.NextText(&reader)
        }

        if wsldb.NextType(&reader) != wsldb.Null {
            atm.Address = wsldb.NextText(&reader)
        }

        if wsldb.NextType(&reader) != wsldb.Null {
            atm.Country = wsldb.NextText(&reader)
        }

        if wsldb.NextType(&reader) != wsldb.Null {
            atm.City = wsldb.NextText(&reader)
        }

        if wsldb.NextType(&reader) != wsldb.Null {
            atm.State = wsldb.NextText(&reader)
        }

        if wsldb.NextType(&reader) != wsldb.Null {
            atm.ZipCode = wsldb.NextInt8(&reader)
        }

        atms = append(atms, atm)
    }

    return atms
}

func ApiMiddleware(conn *net.TCPConn) gin.HandlerFunc {
    return func(c *gin.Context) {
        c.Set("tcpConn", conn)
        c.Next()
    }
}

func main() {
    conn := wsldb.ConnectToServer("localhost:3000")
    defer conn.Close()

    seed, err := ioutil.ReadFile("seed.sql")
    if err != nil {
        os.Exit(1)
    }
    wsldb.ExecuteQuery(conn, string(seed))



    router := gin.Default()
    router.Use(ApiMiddleware(conn))
    router.GET("/atms", getAtms)
    router.GET("/atms/:id", getAtmById)
    router.POST("/atms", postAtms)
    router.Run("localhost:8080")
    /*
    readers := wsldb.ExecuteQuery(conn, "select _rowid from atm_locations where _rowid = 111; select _rowid, bank_name, address from atm_locations where zip_code = 1000 or zip_code = 11011;")

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
                }
            }
            fmt.Printf("\n")
        }
    }*/

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
    }*/
}

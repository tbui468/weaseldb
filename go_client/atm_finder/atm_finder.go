package main

import (
    "wsldb"
    "os"
    "net"
    "io/ioutil"
    "github.com/gin-gonic/gin"
    "net/http"
    "encoding/json"
    "strconv"
)

type Atm struct {
    Id          int64   `json:"id"`
    BankName    string  `json:"bank_name"`
    Address     string  `json:"address"`
    Country     string  `json:"country"`
    City        string  `json:"city"`
    State       string  `json:"state"`
    ZipCode     string  `json:"zip_code"`
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
    c.IndentedJSON(http.StatusOK, atms[0])
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
    values := Quote(a.BankName) + ", " + Quote(a.Address) + ", " + Quote(a.Country) + ", "  + Quote(a.City) + ", " + Quote(a.State) + ", " + a.ZipCode
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

func updateAtmById(c *gin.Context) {
    conn, ok := c.MustGet("tcpConn").(*net.TCPConn)
    if !ok {
        os.Exit(1);
    }

    jsondata, err := ioutil.ReadAll(c.Request.Body)
    if err != nil {
        os.Exit(1)
    }

    var dat map[string]interface{}
    if err := json.Unmarshal(jsondata, &dat); err != nil {
        os.Exit(1)
    }

    set := ""
    count := 0
    for k, v := range dat {
        switch k {
        case "bank_name":
            set += "bank_name = " + Quote(v.(string))
        case "address":
            set += "address = " + Quote(v.(string))
        case "country":
            set += "country = " + Quote(v.(string))
        case "city":
            set += "city = " + Quote(v.(string))
        case "state":
            set += "state = " + Quote(v.(string))
        case "zip_code":
            set += "zip_code = " + v.(string)
        default:
            os.Exit(1)
        }
        if count < len(dat) - 1 {
            set += ", "
        }
        count += 1
    }

    id := c.Param("id")
    query := "update atm_locations set " + set + " where _rowid = " + string(id) + ";"
    sel := "select _rowid, bank_name, address, country, city, state, zip_code from atm_locations where _rowid = " + string(id) + ";"
    readers := wsldb.ExecuteQuery(conn, "begin; " + query + sel + " commit;")

    for _, reader := range readers {
        if reader.RowCount != 0 {
            atms := FillStruct(reader)
            c.IndentedJSON(http.StatusOK, atms[0])
            return
        }
    }

    c.Status(http.StatusOK)
}

func deleteAtmById(c *gin.Context) {
    conn, ok := c.MustGet("tcpConn").(*net.TCPConn)
    if !ok {
        os.Exit(1);
    }

    id := c.Param("id")
    wsldb.ExecuteQuery(conn, "delete from atm_locations where _rowid = " + string(id) + ";")
    c.Status(http.StatusOK)
}

func FillStruct(reader wsldb.Reader) []Atm {
    atms := make([]Atm, 0)

    for row := 0; row < reader.RowCount; row++ {
        atm := Atm{Id: 0, BankName: "", Address: "", Country: "", City: "", State: "", ZipCode: ""}

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
            atm.ZipCode = strconv.FormatInt((wsldb.NextInt8(&reader)), 10)
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
    router.PATCH("/atms/:id", updateAtmById)
    router.DELETE("/atms/:id", deleteAtmById)
    router.Run("localhost:8080")
}

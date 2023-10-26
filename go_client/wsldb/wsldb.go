package wsldb

//run go client in two ways:
//  run with sql script
//  run without argument, and use command line to enter sql
//Run go_client with tests
//  may be a problem with float4 being printed in a incompatible format
//make this into a package that we can import into the backend app
//Do the welcome tour of Go for a quick rundown of syntax and features
//  https://go.dev/tour/list

import (
    "net"
    "os"
    "encoding/binary"
    "reflect"
    "math"
    "fmt"
)

const (
   Int8 int = 0
   Float4   = 1
   Text     = 2
   Bool     = 3
   Null     = 4
)

type RowDescription struct {
    col_count int
    names []string
    types []int
}

func DatumTypeToString(dt int) string {
    switch dt {
        case Int8:
            return "Int8"
        case Float4:
            return "Float4"
        case Text:
            return "Text"
        case Bool:
            return "Bool"
        case Null:
            return "Null"
        default:
            return "Invalid type"
    }
}

func PrepareQuery(s string) []byte {
    msg_type := []byte("Q")
    msg_size := make([]byte, 4)
    var a int32 = 0
    binary.LittleEndian.PutUint32(msg_size, uint32(len(s)) + uint32(reflect.TypeOf(a).Size()))
    query := []byte(s)
    return append(append(msg_type, msg_size...), query...)
}

func ConnectToServer(addr string) *net.TCPConn {
    tcp_addr, err := net.ResolveTCPAddr("tcp", addr)

    if err != nil {
        println("ResolveTCPAddr failed:", err.Error())
        os.Exit(1);
    }

    conn, err := net.DialTCP("tcp", nil, tcp_addr)
    if err != nil {
        println("DialTCP failed:", err.Error())
        os.Exit(1)
    }

    return conn
}

func Write(conn *net.TCPConn, buf []byte) {
    sent := 0

    for sent < len(buf) {
        n, err := conn.Write(buf)
        if err != nil {
            println("Write failed:", err.Error())
            os.Exit(1)
        }

        sent += n
    }
}

func Read(conn *net.TCPConn, buf []byte) []byte {
    tmp := make([]byte, 256)

    for len(buf) < 5 {
        n, err := conn.Read(tmp)
        if err != nil {
            println("Read failed:", err.Error())
        }
        buf = append(buf, tmp[:n]...)
    }

    size := binary.LittleEndian.Uint32(buf[1:5]) + 1

    tmp = make([]byte, 256)

    for len(buf) < int(size) {
        n, err := conn.Read(tmp)
        if err != nil {
            println("Read failed:", err.Error())
        }
        buf = append(buf, tmp[:n]...)
    }

    return buf
}

func ReadSize(buf []byte, off int) (int, int) {
    result := binary.LittleEndian.Uint32(buf[off: off + 4])

    return int(result), off + 4
}

func ReadInt8(buf []byte, off int) (int64, int) {
    result := binary.LittleEndian.Uint64(buf[off: off + 8])

    return int64(result), off + 8
}

func ReadFloat4(buf []byte, off int) (float32, int) {
    result := binary.LittleEndian.Uint32(buf[off: off + 4])
    return math.Float32frombits(result), off + 4
}

func ReadBool(buf []byte, off int) (bool, int) {
    result := buf[off] == 1

    return result, off + 1
}

func ReadText(buf []byte, off int) (string, int) {
    var size int
    size, off = ReadSize(buf, off)
    name := string(buf[off: off + size])

    return name, off + size
}

func ProcessResponse(conn *net.TCPConn, buf []byte, rd RowDescription) (bool, []byte, RowDescription) {
    for len(buf) < 5 {
        buf = Read(conn, buf)
    }

    code := string(buf[0:1])
    size := binary.LittleEndian.Uint32(buf[1:5])

    for len(buf) < int(size) + 1 {
        buf = Read(conn, buf)
    }

    msg := buf[5:size + 1]
    off := 0
    switch code {
        case "T":
            rd = RowDescription{col_count: 0, names: make([]string, 0), types: make([]int, 0)}
            rd.col_count, off = ReadSize(msg, off)
            for i := 0; i < rd.col_count; i++ {
                var dt int
                var name string
                dt, off = ReadSize(msg, off)
                name, off = ReadText(msg, off)
                rd.names = append(rd.names, name)
                rd.types = append(rd.types, dt)
            }
        case "D":
            for i := 0; i < rd.col_count; i++ {
                var is_null bool
                is_null, off = ReadBool(msg, off)
                if is_null {
                    fmt.Print("null,")
                    continue
                }

                switch (rd.types[i]) {
                case Int8:
                    var i int64
                    i, off = ReadInt8(msg, off)
                    fmt.Printf("%d,", i)
                case Float4:
                    var i float32
                    i, off = ReadFloat4(msg, off)
                    fmt.Printf("%.1f,", i)
                case Text:
                    var text string
                    text, off = ReadText(msg, off)
                    fmt.Printf("%s,", text)
                case Bool:
                    var b bool
                    b, off = ReadBool(msg, off)
                    if b {
                        fmt.Print("true,")
                    } else {
                        fmt.Print("false,")
                    }
                case Null:
                    print("[Error],")
                default:
                    print("[Error],")
                }
            }
            fmt.Print("\n")
        case "C":
            //println("query successfully completed")
        case "E":
            //fmt.Println(string(msg))
        case "Z":
            return true, buf[1 + size:], rd
        default:
            println("not implemented yet")
    }

    return false, buf[1 + size:], rd
}

func ProcessQuery(conn *net.TCPConn, query string) {
    packet := PrepareQuery(query)
    Write(conn, packet)

    buf := make([]byte, 0, 1024)
    rd := RowDescription{col_count: 0, names: make([]string, 0), types: make([]int, 0)}

    for {
        var end bool
        end, buf, rd = ProcessResponse(conn, buf, rd)
        if end {
            break
        }
    }

}

/*
func main() {
    args := os.Args[1:]

    conn := ConnectToServer("localhost:3000")
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
            ProcessQuery(conn, text)
        }
    case 1:
        bytes, err := ioutil.ReadFile(args[0])
        if err != nil {
            os.Exit(1)
        }

        ProcessQuery(conn, string(bytes))
    default:
        println("Usage: go_client [filename]")
        os.Exit(1)
    }
}
*/







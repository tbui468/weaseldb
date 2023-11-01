package wsldb

import (
    "net"
    "os"
    "encoding/binary"
    "reflect"
    "math"
)

const (
   Int8 int = 0
   Float4   = 1
   Text     = 2
   Bool     = 3
   Null     = 4
   Bytea    = 5
)

type RowDescription struct {
    col_count int
    names []string
    types []int
}

type Reader struct {
    RowCount int
    ColCount int
    buf []byte
    idx int
}

func AtEnd(reader *Reader) bool {
    return reader.idx >= len(reader.buf)
}

func NextType(reader *Reader) int {
    result := binary.LittleEndian.Uint32(reader.buf[reader.idx: reader.idx + 4])
    reader.idx += 4
    return int(result)
}


func NextInt8(reader *Reader) int64 {
    result := binary.LittleEndian.Uint64(reader.buf[reader.idx: reader.idx + 8])
    reader.idx += 8
    return int64(result)
}

func NextFloat4(reader *Reader) float32 {
    result := binary.LittleEndian.Uint32(reader.buf[reader.idx: reader.idx + 4])
    reader.idx += 4
    return math.Float32frombits(result)
}

func NextBool(reader *Reader) bool {
    result := reader.buf[reader.idx] == 1
    reader.idx += 1
    return result
}

func NextSize(reader *Reader) int {
    result := binary.LittleEndian.Uint32(reader.buf[reader.idx: reader.idx + 4])
    reader.idx += 4
    return int(result)
}

func NextText(reader *Reader) string {
    size := NextSize(reader)
    text := string(reader.buf[reader.idx: reader.idx + size])
    reader.idx += size
    return text
}

func NextBytea(reader *Reader) string {
    size := NextSize(reader)
    text := string(reader.buf[reader.idx: reader.idx + size])
    reader.idx += size
    return text
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
        case Bytea:
            return "Bytea"
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

func ProcessResponse(conn *net.TCPConn, buf []byte, rd RowDescription, reader Reader, readers *[]Reader) (bool, []byte, RowDescription, Reader) {
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
            reader.ColCount = rd.col_count
            for i := 0; i < rd.col_count; i++ {
                var dt int
                dt, off = ReadSize(msg, off)

                var size int
                size, off = ReadSize(msg, off)
                name := string(buf[off: off + size])
                off += size

                rd.names = append(rd.names, name)
                rd.types = append(rd.types, dt)
            }
        case "D":
            reader.RowCount += 1
            for i := 0; i < rd.col_count; i++ {
                is_null := msg[off] == 1
                off += 1
                if is_null {
                    buf := make([]byte, 4)
                    binary.LittleEndian.PutUint32(buf, Null)
                    reader.buf = append(reader.buf, buf...)
                    continue
                }

                buf := make([]byte, 4)
                binary.LittleEndian.PutUint32(buf, uint32(rd.types[i]))
                reader.buf = append(reader.buf, buf...)

                switch (rd.types[i]) {
                case Int8:
                    reader.buf = append(reader.buf, msg[off: off + 8]...)
                    off += 8
                case Float4:
                    reader.buf = append(reader.buf, msg[off: off + 4]...)
                    off += 4
                case Text:
                    string_size := int(binary.LittleEndian.Uint32(msg[off: off + 4]))
                    size := 4 + string_size
                    reader.buf = append(reader.buf, msg[off: off + size]...)
                    off += size
                case Bool:
                    reader.buf = append(reader.buf, msg[off])
                    off += 1
                case Bytea:
                    string_size := int(binary.LittleEndian.Uint32(msg[off: off + 4]))
                    size := 4 + string_size
                    reader.buf = append(reader.buf, msg[off: off + size]...)
                    off += size
                case Null:
                    print("[Error],")
                default:
                    print("[Error],")
                }
            }
        case "C":
            //TODO: something broken here
            //println("query successfully completed")
            if reader.RowCount > 0 {
                *readers = append(*readers, reader)
            }
            reader = Reader{RowCount: 0, ColCount: 0, buf: make([]byte, 0), idx: 0}
        case "E":
            //fmt.Println(string(msg))
        case "Z":
            return true, buf[1 + size:], rd, reader
        default:
            println("not implemented yet")
    }

    return false, buf[1 + size:], rd, reader
}

func ExecuteQuery(conn *net.TCPConn, query string) []Reader {
    packet := PrepareQuery(query)
    Write(conn, packet)

    buf := make([]byte, 0, 1024)
    rd := RowDescription{col_count: 0, names: make([]string, 0), types: make([]int, 0)}
    reader := Reader{RowCount: 0, ColCount: 0, buf: make([]byte, 0), idx: 0}
    readers := make([]Reader, 0)

    for {
        var end bool
        //TODO: ProcessResponse should 
        end, buf, rd, reader = ProcessResponse(conn, buf, rd, reader, &readers)
        if end {
            break
        }
    }

    return readers
}


package find_first_unique

import (
	"fmt"
	"io"
)

// Record format
// content length 32 bytes | content n bytes
type Record struct {
	Len   uint32
	Value []byte
}

func NewRecord(value string) *Record {
	v := []byte(value)
	return &Record{
		Len:   uint32(len(v)),
		Value: v,
	}
}

type RecordReader struct {
	reader io.Reader
}

type RecordWriter struct {
	writer io.Writer
}

func NewRecordReader(reader io.Reader) *RecordReader {
	return &RecordReader{
		reader: reader,
	}
}

func NewRecordWriter(writer io.Writer) *RecordWriter {
	return &RecordWriter{
		writer: writer,
	}
}

func (r *RecordReader) ReadRecord() (record *Record, eof bool) {
	buff := make([]byte, 4)
	n, err := r.reader.Read(buff)
	if n != 4 || err != nil {
		if err == io.EOF {
			return nil, true
		}
		panic(fmt.Errorf("ReadRecord Failed: n = %d; want 4, err = %v", n, err))
	}
	length := BytesToUint32(buff)
	buff = make([]byte, length)
	n, err = r.reader.Read(buff)
	if uint32(n) != length || err != nil {
		panic(fmt.Errorf("ReadRecord Failed: n = %d; want %d, err = %v", n, length, err))
	}
	record = &Record{
		Len:   length,
		Value: buff,
	}
	return
}

func (w *RecordWriter) WriteRecord(record *Record) {
	buff := Uint32ToBytes(record.Len)
	n, err := w.writer.Write(buff)
	if n != 4 || err != nil {
		panic(fmt.Errorf("WriteRecord Failed: n = %d; want 4, err = %v", n, err))
	}
	buff = record.Value
	n, err = w.writer.Write(buff)
	if uint32(n) != record.Len || err != nil {
		panic(fmt.Errorf("WriteRecord Failed: n = %d; want %d, err = %v", n, record.Len, err))
	}
}

// from high bits to low bits
func Uint32ToBytes(v uint32) []byte {
	buff := make([]byte, 4)
	buff[0] = byte(v >> 24)
	buff[1] = byte(v >> 16)
	buff[2] = byte(v >> 8)
	buff[3] = byte(v)
	return buff
}

func BytesToUint32(v []byte) uint32 {
	var num uint32
	num = uint32(v[0])<<24 + uint32(v[1])<<16 + uint32(v[2])<<8 + uint32(v[3])
	return num
}

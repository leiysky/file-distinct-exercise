package find_first_unique

import (
	"bytes"
	"os"
	"testing"
)

func TestRecord(t *testing.T) {
	f, err := os.Create("data")
	if err != nil {
		panic(err)
	}
	w := NewRecordWriter(f)
	records := []*Record{
		NewRecord("hello"),
		NewRecord("world"),
		NewRecord("HOW"),
		NewRecord("ARE"),
		NewRecord("YOU?"),
		NewRecord("你好"),
	}
	for _, record := range records {
		w.WriteRecord(record)
	}
	var i int
	f.Close()
	f, err = os.Open("data")
	if err != nil {
		panic(err)
	}
	r := NewRecordReader(f)
	for record, eof := r.ReadRecord(); !eof; record, eof = r.ReadRecord() {
		if bytes.Compare(record.Value, records[i].Value) != 0 || record.Len != records[i].Len {
			t.Errorf("ReadRecord error: want %v; get %v", records[i].Value, record.Value)
		}
		i++
	}
	os.Remove("data")
}

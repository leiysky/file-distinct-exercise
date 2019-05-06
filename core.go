package find_first_unique

import (
	"math/rand"
	"os"
)

const (
	recordAmounts = 10000
)

type OrderedMap struct {
	htable map[string]*Node
	list   *List
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		htable: make(map[string]*Node),
		list:   NewList(),
	}
}

type RecordMeta struct {
	offset    int64
	duplicate bool
}

func (m *OrderedMap) Put(k string, v interface{}) {
	if _, ok := m.htable[k]; ok {
		m.htable[k].value = v
		return
	}
	node := m.list.PushBack(v)
	m.htable[k] = node
}

func (m *OrderedMap) Get(k string) (v interface{}, success bool) {
	_, success = m.htable[k]
	if success {
		v = m.htable[k].value
	}
	return
}

func (m *OrderedMap) GetFirst() (v interface{}, success bool) {
	node := m.list.head
	if node == nil {
		return
	}
	v = node.value
	success = true
	return
}

func (m *OrderedMap) PopFirst() {
	m.list.PopFront()
}

func initData() {
	f, err := os.Create("data")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := NewRecordWriter(f)
	for i := 0; i < recordAmounts; i++ {
		w.WriteRecord(NewRecord(RandStringBytes(rand.Intn(4096))))
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// func FindFirstUniqueRecord() *Record {
// 	f, err := os.Open("data")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer f.Close()
// 	r := NewRecordReader(f)
// 	m := NewOrderedMap()
// 	var offset int64
// 	for record, eof := r.ReadRecord(); !eof; record, eof = r.ReadRecord() {
// 		h := md5.New()
// 		h.Write(record.Value)
// 		// calculate the signature to identify a record
// 		sig := string(h.Sum(nil))
// 		if _, ok := m.Get(sig); ok {
// 			m.Put(sig, RecordMeta{
// 				duplicate: true,
// 			})
// 		} else {
// 			m.Put(sig, RecordMeta{
// 				offset: offset,
// 			})
// 		}
// 		offset += int64(record.Len + 4)
// 	}
// 	// lookup the first unique value
// 	var result *RecordMeta
// 	for {
// 		v, ok := m.GetFirst()
// 		if !ok {
// 			break
// 		}
// 		meta := v.(RecordMeta)
// 		if !meta.duplicate {
// 			result = &meta
// 			break
// 		}
// 		m.PopFirst()
// 	}
// 	if result == nil {
// 		return nil
// 	}
// 	_, err = f.Seek(result.offset, 0)
// 	if err != nil {
// 		panic(err)
// 	}
// 	record, _ := r.ReadRecord()
// 	return record
// }

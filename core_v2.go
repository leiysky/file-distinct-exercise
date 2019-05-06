package find_first_unique

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
)

var (
	maxFileSize = 4 * 1024 * 1024 // 4GB
)

var (
	ErrSortedFileFull   error = errors.New("file is oversize")
	ErrSortedFileClosed error = errors.New("file has been closed")
)

// SortedBuffer format
// content n bytes | id uint32 4 bytes
type SortedBuffer [][]byte

func (b SortedBuffer) Len() int {
	return len(b)
}

func (b SortedBuffer) Less(i, j int) bool {
	// we just compare the real content
	if bytes.Compare(b[i][:len(b[i])-4], b[j][:len(b[j])-4]) < 0 {
		return true
	}
	return false
}

func (b SortedBuffer) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func compareSortedBuffer(a, b []byte) int {
	return bytes.Compare(a[:len(a)-4], b[:len(b)-4])
}

type FileMeta struct {
	filename string
	smallest []byte
}

type SortedFile struct {
	file        *os.File
	meta        *FileMeta
	buffer      SortedBuffer
	currentSize int
	closed      bool
}

type SortedFileIterator struct {
	reader *RecordReader
	// phantomNext   *Record
	currentRecord *Record
	nextRecord    *Record
}

func NewSortedFileIterator(file *os.File) *SortedFileIterator {
	reader := NewRecordReader(file)
	return &SortedFileIterator{
		reader: reader,
	}
}

// HasNext must called before Next
func (itr *SortedFileIterator) HasNext() bool {
	record, eof := itr.reader.ReadRecord()
	if eof {
		itr.nextRecord = nil
		return false
	}
	itr.nextRecord = record
	return true
}

func (itr *SortedFileIterator) Next() {
	itr.currentRecord = itr.nextRecord
}

func (itr *SortedFileIterator) Get() *Record {
	return itr.currentRecord
}

// handle initial situation
func (itr *SortedFileIterator) GetSmallest() *Record {
	if itr.currentRecord == nil {
		if itr.HasNext() {
			itr.Next()
			return itr.currentRecord
		}
		return nil
	}
	return itr.currentRecord
}

func (itr *SortedFileIterator) EmitSmallest() {
	if itr.HasNext() {
		itr.Next()
		return
	}
	itr.nextRecord = nil
	itr.currentRecord = nil
}

// DUPLICATED: HasUniqueNext is like HasNext.
// If we have phantom, just set nextRecord with phantom and return true.
// If there is 0 element rest, which means no next, return false.
// If there is 1 element rest, which means no duplicte elements rest, return true.
// If there is more than 1 element rest, we should keep `next` and `phantom`.
// func (itr *SortedFileIterator) HasUniqueNext() bool {
// 	if itr.phantomNext != nil {
// 		itr.nextRecord = itr.phantomNext
// 		itr.phantomNext = nil
// 		return true
// 	}
// 	if !itr.HasNext() {
// 		return false
// 	}
// 	// ASSERT: have at least 1 rest
// 	next := itr.nextRecord
// 	if !itr.HasNext() {
// 		itr.nextRecord = next
// 		return true
// 	}
// 	// ASSERT: have at least 2 rest
// 	phantom := itr.nextRecord
// 	if compareSortedBuffer(next.Value, phantom.Value) != 0 {
// 		itr.nextRecord = next
// 		itr.phantomNext = phantom
// 		return true
// 	}
// 	for {
// 		if !itr.HasNext() {
// 			return false
// 		}
// 		itr.Next()
// 		phantom = itr.Get()
// 		if compareSortedBuffer(next.Value, phantom.Value) != 0 {
// 			next = phantom
// 			return true
// 		}
// 	}
// }

func (itr *SortedFileIterator) HasUniqueNext() bool {
	for itr.HasNext() {
		id := BytesToUint32(itr.nextRecord.Value[itr.nextRecord.Len-4:])
		if id != 0 {
			return true
		}
	}
	return false
}

func getFileName(fileNum int) string {
	return fmt.Sprintf("temp/chunk-%d", fileNum)
}

func NewSortedFile(filename string) *SortedFile {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	return &SortedFile{
		file: f,
		meta: &FileMeta{
			filename: filename,
		},
	}
}

func (f *SortedFile) Write(p []byte) (n int, err error) {
	if len(p)+f.currentSize > maxFileSize {
		return 0, ErrSortedFileFull
	}
	f.buffer = append(f.buffer, p)
	f.currentSize += len(p)
	return len(p), nil
}

func (f *SortedFile) Flush() error {
	if f.closed {
		return ErrSortedFileClosed
	}
	f.sortBuffer()
	f.meta.smallest = f.buffer[0]
	w := NewRecordWriter(f.file)
	for _, v := range f.buffer {
		w.WriteRecord(&Record{
			Len:   uint32(len(v)),
			Value: v,
		})
	}
	return f.file.Sync()
}

func (f *SortedFile) Close() error {
	if f.closed {
		return ErrSortedFileClosed
	}
	f.closed = true
	return f.file.Close()
}

func (f *SortedFile) sortBuffer() {
	sort.Sort(f.buffer)
}

func SplitSourceFile(file *os.File) (metas []*FileMeta) {
	err := os.Mkdir("temp", 0777)
	if err != nil {
		panic(err)
	}
	r := NewRecordReader(file)
	var fileNum int
	var recordNum int
	f := NewSortedFile(getFileName(fileNum))
	for {
		record, eof := r.ReadRecord()
		if eof {
			if len(f.buffer) > 0 {
				f.Flush()
				f.Close()
				metas = append(metas, f.meta)
			}
			return metas
		}
		recordNum++
		_, err = f.Write(bytes.Join([][]byte{record.Value, Uint32ToBytes(uint32(recordNum))}, nil))
		if err != nil {
			if err == ErrSortedFileFull {
				f.Flush()
				f.Close()
				metas = append(metas, f.meta)
				fileNum++
				f = NewSortedFile(getFileName(fileNum))
			} else {
				panic(err)
			}
		}
	}
}

// return nil means there is no record
func findSmallest(itrs []*SortedFileIterator) []byte {
	if len(itrs) == 0 {
		return nil
	}
	var smallest []byte
	var idx int
	for i, itr := range itrs {
		if itr == nil {
			continue
		}
		record := itr.GetSmallest()
		if record == nil {
			itrs[i] = nil
			continue
		}
		if smallest == nil {
			smallest = record.Value
			idx = i
			continue
		}
		if compareSortedBuffer(smallest, record.Value) > 0 {
			smallest = record.Value
			idx = i
		}
	}
	if itrs[idx] != nil {
		itrs[idx].EmitSmallest()
	}
	return smallest
}

// Merge and uniquify
func Merge(metas []*FileMeta) {
	f, err := os.Create("temp/data")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if len(metas) <= 0 {
		panic("invalid meta")
	}
	w := NewRecordWriter(f)
	itrs := make([]*SortedFileIterator, len(metas))
	for i, v := range metas {
		f, err := os.Open(v.filename)
		if err != nil {
			panic(err)
		}
		itrs[i] = NewSortedFileIterator(f)
	}

	pending := findSmallest(itrs)
	if pending == nil {
		return
	}
	for {
		smallest := findSmallest(itrs)
		if smallest == nil {
			w.WriteRecord(&Record{
				Len:   uint32(len(pending)),
				Value: pending,
			})
			break
		}
		if compareSortedBuffer(pending, smallest) == 0 {
			// ID = 0 means the record is duplicate
			pending = append(pending[:len(pending)-4], 0, 0, 0, 0)
			continue
		}
		w.WriteRecord(&Record{
			Len:   uint32(len(pending)),
			Value: pending,
		})
		pending = smallest
	}
}

func LookUp() []byte {
	f, err := os.Open("temp/data")
	if err != nil {
		panic(err)
	}
	itr := NewSortedFileIterator(f)

	var id uint32
	var first []byte

	for itr.HasUniqueNext() {
		itr.Next()
		record := itr.Get()
		cur := record.Value[:record.Len-4]
		curID := BytesToUint32(record.Value[record.Len-4:])
		if id > curID || id == 0 {
			id = curID
			first = cur
		}
	}
	return first
}

func FindFirstUniqueRecord() *Record {
	f, err := os.Open("data")
	if err != nil {
		panic(err)
	}
	metas := SplitSourceFile(f)
	if len(metas) == 0 {
		return nil
	}
	Merge(metas)
	result := LookUp()
	if result == nil {
		return nil
	}
	return &Record{Len: uint32(len(result)), Value: result}
}

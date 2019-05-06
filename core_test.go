package find_first_unique

import (
	"os"
	"testing"
)

var (
	testSet1 = []string{
		"hello",
		"world",
		"hello",
	} // result should be world
	testSet2 = []string{
		"hello",
		"world",
		"hello",
		"world",
	} // result should be nil
	testSet3 = []string{} // result should be nil
	testSet4 = []string{
		"hello",
	} // result should be hello
)

func initTestData(data []string) {
	f, err := os.Create("data")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := NewRecordWriter(f)
	for _, v := range data {
		w.WriteRecord(NewRecord(v))
	}
}

func TestSolution(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			os.RemoveAll("temp")
			os.Remove("data")
			panic(err)
		}
	}()

	initTestData(testSet1)
	record := FindFirstUniqueRecord()
	os.RemoveAll("temp")
	os.Remove("data")
	if string(record.Value) != "world" {
		t.Errorf("FindFirstUniqueRecord failed: want record = %v; get record = %v", Record{5, []byte("world")}, record)
	}
	initTestData(testSet2)
	record = FindFirstUniqueRecord()
	os.RemoveAll("temp")
	os.Remove("data")
	if record != nil {
		t.Errorf("FindFirstUniqueRecord failed: want record = nil; get record = %v", record)
	}
	initTestData(testSet3)
	record = FindFirstUniqueRecord()
	os.RemoveAll("temp")
	os.Remove("data")
	if record != nil {
		t.Errorf("FindFirstUniqueRecord failed: want record = nil; get record = %v", record)
	}
	initTestData(testSet4)
	record = FindFirstUniqueRecord()
	os.RemoveAll("temp")
	os.Remove("data")
	if string(record.Value) != "hello" {
		t.Errorf("FindFirstUniqueRecord failed: want record = %v; get record = %v", Record{5, []byte("hello")}, record)
	}
}

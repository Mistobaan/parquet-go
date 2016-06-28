package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"testing"

	"github.com/kostya-sh/parquet-go/parquet"
)

func genBool(num int) []bool {
	r := make([]bool, num)
	for i := 0; i < num; i++ {
		r[i] = rand.Intn(100) > 50
	}
	return r
}

func TestBooleanColumn(t *testing.T) {

	schema := parquet.NewSchema()

	//	fd := parquet.NewFile("tempfile", s)

	//	fd.Close()

	err := schema.AddColumnFromSpec("value: boolean REQUIRED")
	if err != nil {
		t.Fatal(err)
	}

	// tmpfile, err := ioutil.TempFile("", "test_parquet")
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// defer os.Remove(tmpfile.Name()) // clean up

	var b bytes.Buffer

	enc := parquet.NewEncoder(schema, parquet.NopCloser(&b))

	// values := genBool(100)
	// for _, v := range values {
	// 	record := []map[string]interface{}{{"value": v}}
	// 	if err := enc.WriteRecords(record); err != nil {
	// 		t.Fatal(err)
	// 	}
	// }

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	fileName := "./boolean.parquet"

	if err := ioutil.WriteFile(fileName, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	// launch external implementation
	cmd := exec.Command("./parquet_reader", fileName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatal("cmd.Start", err)
	}

	io.Copy(os.Stdout, stdout)
	io.Copy(os.Stdout, stderr)

	if err := cmd.Wait(); err != nil {
		log.Fatal("wait", err)
	}

	// if err := tmpfile.Close(); err != nil {
	// 	t.Fatal(err)
	// }

}

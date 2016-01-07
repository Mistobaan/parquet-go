package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/linkedin/goavro"
)

func check(w int, err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func makeSomeData(w io.Writer) error {
	schema := `
{
    "type": "record",
    "name": "example",
    "fields": [
        {
            "type": "long",
            "name": "long_field"
        },
        {
            "type": "integer",
            "name": "integer_field"
        },
        {
            "type": "decimal",
            "name": "decimal_field"
        },
        {
            "type": "float",
            "name": "float_field"
        },
        {
            "type": "double",
            "name": "double_field"
        },
        {
            "type": "boolean",
            "name": "boolean_field"
        },
        {
            "type": "string",
            "name": "string_field"
        },
        {
            "type": "date",
            "name": "date_field"
        },
        {
            "type": "timestamp ",
            "name": "timestamp_field"
        }
    ]
}
    `
	var err error
	// If you want speed, create the codec one time for each
	// schema and reuse it to create multiple Writer instances.
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}

	fw, err := codec.NewWriter(
		//		goavro.BlockSize(13),                         // example; default is 10
		goavro.Compression(goavro.CompressionSnappy), // default is CompressionNull
		goavro.ToWriter(w))
	if err != nil {
		log.Fatal("avro: cannot create Writer: ", err)
	}
	defer fw.Close()

	records := []map[string]interface{}{
		// GENERATE A Record based on the type.
		{"long_field": float32(1), "integer_field": int(2), "decimal_field": int(3),
			"float_field": float32(4), "double_field": float64(5), "boolean_field": true,
			"string_field": string("7"), "date_field": int64(8), "timestamp_field": int64(9),
		},
	}

	for _, rec := range records {
		record, err := goavro.NewRecord(goavro.RecordSchema(schema))
		if err != nil {
			log.Fatal(err)
		}

		for k, v := range rec {
			record.Set(k, v)
		}

		fw.Write(record)
	}

	return nil
}

func dumpReader(r io.Reader) {
	fr, err := goavro.NewReader(goavro.BufferFromReader(r))
	if err != nil {
		log.Fatal("cannot create Reader: ", err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for fr.Scan() {
		datum, err := fr.Read()
		if err != nil {
			log.Println("cannot read datum: ", err)
			continue
		}
		fmt.Println(datum)
	}
}

func main() {
	fd, err := os.Create("temp.avro")
	if err != nil {
		log.Println("error", err)
	}

	makeSomeData(fd)

	fd.Close()

	fd, err = os.Open("temp.avro")
	if err != nil {
		log.Println("error", err)
	}

	dumpReader(fd)

	fd.Close()

	os.Remove("temp.avro")
	log.Println("finished")
}

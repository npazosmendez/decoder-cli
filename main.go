package main

// a go program that uncompresses snappy data from stdin
// and writes it to stdout

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack"
)

func main() {
	jsonFlag := flag.Bool("msgp", false, "convert msgpack to json")
	snappyFlag := flag.Bool("snappy", false, "uncompress snappy data")

	flag.Parse()

	process := snappyUncompress
	if *jsonFlag {
		process = msgpack2json
	}
	if *snappyFlag {
		process = snappyUncompress
	}

	if flag.NArg() > 0 {
		for _, filename := range flag.Args() {
			f, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			if err := process(f, os.Stdout); err != nil {
				log.Fatal(err)
			}
		}
	} else {
		if err := process(os.Stdin, os.Stdout); err != nil {
			log.Fatal(err)
		}
	}
}

func snappyUncompress(r io.Reader, w io.Writer) error {
	s := snappy.NewReader(r)
	if _, err := io.Copy(w, s); err != nil {
		return fmt.Errorf("snappyUncompress: %v", err)
	}
	return nil
}

func msgpack2json(r io.Reader, w io.Writer) error {
	d := msgpack.NewDecoder(r)

	for {
		var v interface{}
		err := d.Decode(&v)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("msgpack2json: %v", err)
		}

		e := json.NewEncoder(w)
		e.SetIndent("", "  ")
		if err := e.Encode(v); err != nil {
			return fmt.Errorf("msgpack2json: %v", err)
		}
	}
	return nil
}

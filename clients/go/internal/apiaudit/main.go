// apiaudit checks the reviewed Rust-to-Go mapping and public source inventory.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
)

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func main() {
	symbolsOnly := flag.Bool("symbols", false, "print Go symbols and JSON field names")
	current := flag.String("inventory", "api-inventory.json", "fresh Rust inventory to compare")
	flag.Parse()
	symbols := map[string]string{}
	files, err := filepath.Glob("*.go")
	fatal(err)
	for _, path := range files {
		if len(path) >= 8 && path[len(path)-8:] == "_test.go" {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		fatal(err)
		for _, d := range f.Decls {
			switch x := d.(type) {
			case *ast.FuncDecl:
				if !x.Name.IsExported() {
					continue
				}
				name := x.Name.Name
				if x.Recv != nil {
					t := x.Recv.List[0].Type
					if p, ok := t.(*ast.StarExpr); ok {
						t = p.X
					}
					if p, ok := t.(*ast.IndexExpr); ok {
						t = p.X
					}
					if id, ok := t.(*ast.Ident); ok {
						name = id.Name + "." + name
					} else {
						continue
					}
				}
				symbols[name] = ""
			case *ast.GenDecl:
				for _, spec := range x.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if !s.Name.IsExported() {
							continue
						}
						name := s.Name.Name
						symbols[name] = ""
						var fields *ast.FieldList
						switch t := s.Type.(type) {
						case *ast.StructType:
							fields = t.Fields
						case *ast.InterfaceType:
							fields = t.Methods
						}
						if fields != nil {
							for _, field := range fields.List {
								for _, id := range field.Names {
									if id.IsExported() {
										tag := ""
										if field.Tag != nil {
											v, e := strconv.Unquote(field.Tag.Value)
											fatal(e)
											tag = reflect.StructTag(v).Get("json")
										}
										symbols[name+"."+id.Name] = tag
									}
								}
							}
						}
					case *ast.ValueSpec:
						for _, id := range s.Names {
							if id.IsExported() {
								symbols[id.Name] = ""
							}
						}
					}
				}
			}
		}
	}
	if *symbolsOnly {
		b, err := json.MarshalIndent(symbols, "", "  ")
		fatal(err)
		fmt.Println(string(b))
		return
	}
	var inventory, reviewed []string
	b, err := os.ReadFile(*current)
	fatal(err)
	fatal(json.Unmarshal(b, &inventory))
	b, err = os.ReadFile("api-inventory.json")
	fatal(err)
	fatal(json.Unmarshal(b, &reviewed))
	if !reflect.DeepEqual(inventory, reviewed) {
		fatal(fmt.Errorf("Rust public API changed: regenerate inventory, review bindings, and update coverage"))
	}
	var coverage map[string]string
	b, err = os.ReadFile("api-coverage.json")
	fatal(err)
	fatal(json.Unmarshal(b, &coverage))
	missing := []string{}
	for _, rust := range inventory {
		goName, ok := coverage[rust]
		if !ok {
			missing = append(missing, rust+": no coverage mapping")
			continue
		}
		if _, ok := symbols[goName]; !ok {
			missing = append(missing, rust+": missing Go symbol "+goName)
		}
	}
	if len(coverage) != len(inventory) {
		missing = append(missing, "coverage includes obsolete or missing entries")
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		for _, line := range missing {
			fmt.Fprintln(os.Stderr, line)
		}
		os.Exit(1)
	}
	fmt.Printf("Reviewed %d Rust symbols; every binding target exists.\n", len(inventory))
}

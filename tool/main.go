package main

import (
	"os"

	"github.com/marcgauthier/murmur/tool/cmd"
)

func main() {
	code := cmd.Execute(os.Args[1:], os.Stdout, os.Stderr)
	os.Exit(code)
}

package format

import (
	"fmt"
	"io"
	"strings"
)

// RenderTable prints an aligned ASCII or Markdown table.
func RenderTable(w io.Writer, headers []string, rows [][]string, markdown bool) {
	if len(headers) == 0 {
		return
	}

	colWidths := make([]int, len(headers))
	for i, h := range headers {
		colWidths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(colWidths) && len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	if markdown {
		// Header row
		fmt.Fprintf(w, "|")
		for i, h := range headers {
			fmt.Fprintf(w, " %-*s |", colWidths[i], h)
		}
		fmt.Fprintln(w)

		// Separator
		fmt.Fprintf(w, "|")
		for _, width := range colWidths {
			fmt.Fprintf(w, " %s |", strings.Repeat("-", width))
		}
		fmt.Fprintln(w)

		// Rows
		for _, row := range rows {
			fmt.Fprintf(w, "|")
			for i := range headers {
				cell := ""
				if i < len(row) {
					cell = row[i]
				}
				fmt.Fprintf(w, " %-*s |", colWidths[i], cell)
			}
			fmt.Fprintln(w)
		}
		return
	}

	// Border builder for standard table
	printBorder := func(left, mid, right, fill string) {
		fmt.Fprint(w, left)
		for i, width := range colWidths {
			fmt.Fprint(w, strings.Repeat(fill, width+2))
			if i < len(colWidths)-1 {
				fmt.Fprint(w, mid)
			}
		}
		fmt.Fprintln(w, right)
	}

	printBorder("+", "+", "+", "-")

	// Header row
	fmt.Fprint(w, "|")
	for i, h := range headers {
		fmt.Fprintf(w, " %-*s |", colWidths[i], h)
	}
	fmt.Fprintln(w)

	printBorder("+", "+", "+", "-")

	// Data rows
	for _, row := range rows {
		fmt.Fprint(w, "|")
		for i := range headers {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			fmt.Fprintf(w, " %-*s |", colWidths[i], cell)
		}
		fmt.Fprintln(w)
	}

	printBorder("+", "+", "+", "-")
}

// RenderKV prints aligned Key-Value pairs.
func RenderKV(w io.Writer, pairs [][2]string) {
	maxKeyLen := 0
	for _, pair := range pairs {
		if len(pair[0]) > maxKeyLen {
			maxKeyLen = len(pair[0])
		}
	}

	for _, pair := range pairs {
		if pair[0] == "" && pair[1] == "" {
			fmt.Fprintln(w)
			continue
		}
		fmt.Fprintf(w, "%-*s : %s\n", maxKeyLen, pair[0], pair[1])
	}
}

package harness

import (
	"github.com/marcgauthier/murmur/ids"
	"time"
)

// ModelRow uses the runtime model API in a separate live node process.
type ModelRow struct {
	ID     ids.RowID `json:"id"`
	Name   string    `json:"name"`
	Site   string    `json:"site"`
	Status int       `json:"status"`
	Online bool      `json:"online"`
	Host   string    `json:"host"`
	Seen   time.Time `json:"seen"`
}

func (c *Cluster) ModelOperation(idx int, operation string, row ModelRow, rows []ModelRow) (ModelRow, []ModelRow, error) {
	var result struct {
		Row  ModelRow   `json:"row"`
		Rows []ModelRow `json:"rows"`
	}
	err := c.typedRequest(idx, "/v1/typed/models", map[string]any{"operation": operation, "row": row, "rows": rows}, &result)
	return result.Row, result.Rows, err
}

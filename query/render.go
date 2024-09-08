package query

import (
	"fmt"
	"strings"
)

const (
	topLeft        = "┌"
	topRight       = "┐"
	bottomLeft     = "└"
	bottomRight    = "┘"
	horizontal     = "─"
	vertical       = "│"
	tJunction      = "┬"
	leftJunction   = "├"
	rightJunction  = "┤"
	bottomJunction = "┴"
	cross          = "┼"
	TextColWidth   = 20
	IntColWidth    = 8
	BoolColWidth   = 8
)

type Table struct {
	TableName string
	cols      []string
}

type TableConf struct {
	cols    int
	colconf []ColConf
}
type ColConf struct {
	colName  string
	colWidth int
}

func FilterRowsAndRenderTable(allRecs []ColumnValues, rows []int, table Table) {

	// for key, col := range allRecs {
	// 	for _, row := range rows {
	// 		// schema.Columns[key].valtype

	// 		// filter := allRecs[col][row]

	// 	}
	t := MakeColConfig(table)
	fmt.Print(t.RenderColHeader(table.TableName))
	fmt.Print(t.RenderRows(allRecs, rows))
	fmt.Print(t.RenderFooter())

	// }

}

func MakeColConfig(table Table) TableConf {
	tableconf := TableConf{cols: 0}
	for _, v := range table.cols {
		colconfig := ColConf{colName: v}
		switch schema.Columns[v].valtype {
		case Text:

			if len(v) > TextColWidth {
				colconfig.colWidth = len(v) + 2
			} else {
				colconfig.colWidth = TextColWidth

			}
			tableconf.colconf = append(tableconf.colconf, colconfig)
		case Integer:
			if len(v) > IntColWidth {
				colconfig.colWidth = len(v) + 2
			} else {
				colconfig.colWidth = IntColWidth

			}
			tableconf.colconf = append(tableconf.colconf, colconfig)
		case Boolean:
			if len(v) > BoolColWidth {
				colconfig.colWidth = len(v) + 2
			} else {
				colconfig.colWidth = BoolColWidth

			}
			tableconf.colconf = append(tableconf.colconf, colconfig)

		}
		tableconf.cols++
	}
	return tableconf
}
func (t *TableConf) RenderColHeader(tablename string) string {
	var sb strings.Builder

	sb.WriteString(topLeft + " " + fmt.Sprintf("\x1b[1;34m%s", tablename) + " ")
	sb.WriteString("\x1b[0m")
	for i, col := range t.colconf {
		if i == 0 {
			sb.WriteString(strings.Repeat(horizontal, col.colWidth-(len(tablename))))

		} else {

			sb.WriteString(strings.Repeat(horizontal, col.colWidth+2))
		}
		if i < t.cols-1 {
			sb.WriteString(tJunction)
		}
	}
	sb.WriteString(topRight + "\n")
	// write header
	sb.WriteString(vertical)
	for i, col := range t.colconf {
		sb.WriteString(" " + col.colName + " " + strings.Repeat(" ", col.colWidth-(len(col.colName))))
		if i < t.cols-1 {
			sb.WriteString(vertical)
		}
	}
	sb.WriteString(vertical + "\n")
	sb.WriteString(leftJunction)
	for i, col := range t.colconf {
		sb.WriteString(strings.Repeat(horizontal, col.colWidth+2))
		if i < t.cols-1 {
			sb.WriteString(cross)
		}
	}

	sb.WriteString(rightJunction + "\n")
	// draw line

	return sb.String()
}
func (t *TableConf) RenderRows(allRecs []ColumnValues, rows []int) string {
	var sb strings.Builder
	for _, v := range rows {
		sb.WriteString(vertical)
		for i, col := range t.colconf {
			val := allRecs[v][col.colName].val
			if len(val) > col.colWidth {
				val = val[:col.colWidth-3]
				val += "..."

			}
			sb.WriteString(" " + val + " " + strings.Repeat(" ", col.colWidth-(len(val))))
			if i < t.cols-1 {
				sb.WriteString(vertical)
			}

		}
		sb.WriteString(vertical + "\n")
	}
	return sb.String()
}

func (t *TableConf) RenderFooter() string {
	var sb strings.Builder
	sb.WriteString(bottomLeft)
	for i, col := range t.colconf {
		sb.WriteString(strings.Repeat(horizontal, col.colWidth+2))

		if i < t.cols-1 {
			sb.WriteString(bottomJunction)
		}
	}
	sb.WriteString(bottomRight + "\n")
	return sb.String()
}

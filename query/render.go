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

func FilterRowsAndRenderTable(allRecs []ColumnValues, table Table) {

	// for key, col := range allRecs {
	// 	for _, row := range rows {
	// 		// schema.Columns[key].valtype

	// 		// filter := allRecs[col][row]

	// 	}
	t := MakeColConfig(table)
	fmt.Print(t.RenderColHeader(table.TableName))
	fmt.Print(t.RenderRows(allRecs))
	fmt.Print(t.RenderFooter())
	if len(allRecs) == 0 {
		fmt.Println(" No records found. ")
	}

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

	sb.WriteString(topLeft)
	// tname := fmt.Sprintf(" \x1b[1;34jm%s \x1b[0m", tablename)
	tname := fmt.Sprintf(" %s ", tablename)
	// fmt.Print("\x1b[1;34m")
	totalWidth := 0
	for _, col := range t.colconf {
		totalWidth += col.colWidth + 2 // +2 for padding
	}
	totalWidth -= 1
	if len(tname) > totalWidth-2 {
		tname = tname[:totalWidth-5] + "..."
	}
	for i, col := range t.colconf {
		if len(tname) >= 0 {
			var temp string
			if len(tname) >= col.colWidth {
				temp = tname[:col.colWidth+2]
				tname = tname[col.colWidth+2:]

			} else {
				temp = tname
				tname = ""
			}
			r := (col.colWidth + 2) - len(temp)

			sb.WriteString(fmt.Sprintf("\x1b[1;34m%s\x1b[0m", temp) + strings.Repeat(horizontal, r))

		} else {

			sb.WriteString(strings.Repeat(horizontal, col.colWidth+2))
		}
		if i < t.cols-1 && len(tname) == 0 {
			// fmt.Print("\x1b[0m")
			sb.WriteString(tJunction)
		} else if i < t.cols-1 {
			if len(tname) >= 1 {
				sb.WriteString(fmt.Sprintf("\x1b[1;34m%s\x1b[0m", string(tname[0:1])))
				tname = tname[1:]
			}

			// tname = ""
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
func (t *TableConf) RenderRows(allRecs []ColumnValues) string {
	var sb strings.Builder
	for _, v := range allRecs {
		sb.WriteString(vertical)
		for i, col := range t.colconf {
			val := v[col.colName].val
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

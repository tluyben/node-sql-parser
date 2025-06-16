import { columnsToSQL } from './column'
import { exprToSQL, orderOrPartitionByToSQL } from './expr'
import { limitToSQL } from './limit'
import { tablesToSQL } from './tables'
import { commonOptionConnector, hasVal, returningToSQL, getParserOpt } from './util'
import { withToSQL } from './with'

function deleteToSQL(stmt) {
  const { columns, from, table, where, orderby, with: withInfo, limit, returning } = stmt
  const clauses = [withToSQL(withInfo), 'DELETE']
  const { database } = getParserOpt()
  const isClickHouse = database && database.toLowerCase() === 'clickhouse'

  // Handle table references between DELETE and FROM
  // ClickHouse always uses DELETE FROM syntax without table refs
  if (!isClickHouse && columns && columns.length > 0) {
    const tableRefs = columns.map(col => {
      if (col.expr && col.expr.type === 'column_ref') {
        // Use table name if available, otherwise use column name
        if (col.expr.table) {
          return tablesToSQL([{ db: col.expr.db, table: col.expr.table }])
        }
        if (col.expr.column) {
          return `\`${col.expr.column}\``
        }
        return ''
      }
      return columnsToSQL([col], from)
    }).filter(hasVal).join(', ')
    clauses.push(tableRefs)
  } else if (!isClickHouse && table && table.length > 0) {
    // Case 2: DELETE a FROM db.t ... (table contains what to delete)
    // Output table refs when:
    // 1. table has addition = false/undefined (meaning it should be shown)
    // 2. OR table is different from FROM clause (e.g., DELETE a FROM b)
    const shouldShowTable = table.filter(tbl => !tbl.addition)
    if (shouldShowTable.length > 0) {
      clauses.push(tablesToSQL(shouldShowTable))
    }
  }

  clauses.push(commonOptionConnector('FROM', tablesToSQL, from || table))
  clauses.push(commonOptionConnector('WHERE', exprToSQL, where))
  clauses.push(orderOrPartitionByToSQL(orderby, 'order by'))
  clauses.push(limitToSQL(limit))
  clauses.push(returningToSQL(returning))
  return clauses.filter(hasVal).join(' ')
}

export {
  deleteToSQL,
}

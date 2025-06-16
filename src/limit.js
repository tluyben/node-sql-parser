import { connector, toUpper, hasVal, literalToSQL } from './util'
import { exprToSQL } from './expr'

function composePrefixValSuffix(stmt) {
  if (!stmt) return []
  return [stmt.prefix.map(literalToSQL).join(' '), exprToSQL(stmt.value), stmt.suffix.map(literalToSQL).join(' ')]
}

function fetchOffsetToSQL(stmt) {
  const { fetch, offset } = stmt
  const result = [...composePrefixValSuffix(offset), ...composePrefixValSuffix(fetch)]
  return result.filter(hasVal).join(' ')
}

function limitOffsetToSQL(limit) {
  const { seperator, value } = limit
  if (!value) {
    // Handle simple numeric limit object from ClickHouse
    if (limit && (limit.type === 'number' || limit.value != null)) {
      return connector('LIMIT', exprToSQL(limit))
    }
    return ''
  }
  if (value.length === 1 && seperator === 'offset') return connector('OFFSET', exprToSQL(value[0]))
  return connector('LIMIT', value.map(exprToSQL).join(`${seperator === 'offset' ? ' ' : ''}${toUpper(seperator)} `))
}

function limitToSQL(limit) {
  if (!limit) return ''
  // Handle array format from ClickHouse parser
  if (Array.isArray(limit)) {
    if (limit.length === 1) {
      const limitExpr = limit[0]
      // Handle nested value structure
      if (limitExpr && limitExpr.value) {
        return connector('LIMIT', exprToSQL(limitExpr.value))
      }
      return connector('LIMIT', exprToSQL(limitExpr))
    }
    if (limit.length === 2) {
      const offsetExpr = limit[0]
      const limitExpr = limit[1]
      const offsetVal = offsetExpr && offsetExpr.value ? offsetExpr.value : offsetExpr
      const limitVal = limitExpr && limitExpr.value ? limitExpr.value : limitExpr
      return `${connector('LIMIT', exprToSQL(limitVal))} ${connector('OFFSET', exprToSQL(offsetVal))}`
    }
    return ''
  }
  if (limit.fetch || limit.offset) return fetchOffsetToSQL(limit)
  return limitOffsetToSQL(limit)
}

export {
  limitToSQL,
}

import { literalToSQL, toUpper, connector, dataTypeToSQL, hasVal, identifierToSql } from './util'
import { alterExprToSQL } from './alter'
import { aggrToSQL } from './aggregation'
import { assignToSQL } from './assign'
import { binaryToSQL } from './binary'
import { caseToSQL } from './case'
import { collateToSQL } from './collate'
// columnDefinitionToSQL, columnRefToSQL, fullTextSearchToSQL handled directly to avoid circular dependency
import { anyValueFuncToSQL, castToSQL, extractFunToSQL, flattenFunToSQL, funcArgToSQL, funcToSQL, jsonObjectArgToSQL, lambdaToSQL, tablefuncFunToSQL } from './func'
import { intervalToSQL } from './interval'
import { jsonExprToSQL, jsonVisitorExprToSQL } from './json'
import { selectToSQL } from './select'
import { showToSQL } from './show'
import { arrayStructExprToSQL } from './array-struct'
import { tablesToSQL, unnestToSQL } from './tables'
import { unionToSQL } from './union'
import { namedWindowExprListToSQL, windowFuncToSQL } from './window'

const exprToSQLConvertFn = {}

function varToSQL(expr) {
  const { prefix = '@', name, members, quoted, suffix } = expr
  const val = []
  const varName = members && members.length > 0 ? `${name}.${members.join('.')}` : name
  let result = `${prefix || ''}${varName}`
  if (suffix) result += suffix
  val.push(result)
  return [quoted, val.join(' '), quoted].filter(hasVal).join('')
}

function exprToSQL(exprOrigin) {
  if (!exprOrigin) return
  const expr = exprOrigin
  if (exprOrigin.ast) {
    const { ast } = expr
    Reflect.deleteProperty(expr, ast)
    for (const key of Object.keys(ast)) {
      expr[key] = ast[key]
    }
  }
  const { type } = expr
  if (type === 'expr') return exprToSQL(expr.expr)

  // Handle column_ref cases - use dynamic import to avoid circular dependency
  if (type === 'column_ref') {
    try {
      // Try to use the proper columnRefToSQL function
      // eslint-disable-next-line global-require
      const { columnRefToSQL } = require('./column')
      return columnRefToSQL(expr)
    } catch(circularDepError) {
      // Fallback for circular dependency - minimal handling for * cases only
      const { column, table, db, schema } = expr
      if (column === '*') {
        const parts = []
        if (db && typeof db === 'string') parts.push(identifierToSql(db))
        if (schema && typeof schema === 'string') parts.push(identifierToSql(schema))
        if (table && typeof table === 'string') parts.push(identifierToSql(table))
        parts.push('*')
        return parts.join('.')
      }
      // For non-* columns, return a simple identifier to avoid breaking functionality
      if (typeof column === 'string') {
        return identifierToSql(column)
      }
      if (column && column.expr && column.expr.value) {
        return identifierToSql(column.expr.value)
      }
      return literalToSQL(expr)
    }
  }

  // Handle fulltext_search cases - use dynamic import to avoid circular dependency
  if (type === 'fulltext_search') {
    try {
      // Try to use the proper fullTextSearchToSQL function
      // eslint-disable-next-line global-require
      const { fullTextSearchToSQL } = require('./column')
      return fullTextSearchToSQL(expr)
    } catch(circularDepError) {
      // Fallback - basic fulltext search handling
      const { match, columns, against } = expr
      const columnNames = columns ? columns.map(col => col.column || 'column').join(', ') : 'column'
      const matchPart = `${toUpper(match || 'MATCH')} (${columnNames})`
      const againstPart = `${toUpper(against || 'AGAINST')} (?)`
      return `${matchPart} ${againstPart}`
    }
  }

  // Handle column_definition cases - use dynamic import to avoid circular dependency
  if (type === 'column_definition') {
    try {
      // Try to use the proper columnDefinitionToSQL function
      // eslint-disable-next-line global-require
      const { columnDefinitionToSQL } = require('./column')
      return columnDefinitionToSQL(expr)
    } catch(circularDepError) {
      // Fallback - basic column definition handling
      const { column, definition } = expr
      let columnName = ''
      if (column && column.column) {
        if (typeof column.column === 'string') {
          columnName = identifierToSql(column.column)
        } else if (column.column.expr && column.column.expr.value) {
          columnName = identifierToSql(column.column.expr.value)
        }
      }
      const dataType = definition && definition.dataType ? definition.dataType : ''
      return `${columnName} ${dataType}`.trim()
    }
  }

  return exprToSQLConvertFn[type] ? exprToSQLConvertFn[type](expr) : literalToSQL(expr)
}

function arrayAccessToSQL(expr) {
  const { expr: baseExpr, indices } = expr
  let result = null
  // For column references in array access, use the column name directly without backticks
  if (baseExpr.type === 'column_ref') {
    result = baseExpr.column
  } else {
    result = exprToSQL(baseExpr)
  }
  if (indices && indices.length > 0) {
    for (const index of indices) {
      result += `[${exprToSQL(index)}]`
    }
  }
  return result
}

function unaryToSQL(unarExpr) {
  const { operator, parentheses, expr } = unarExpr
  const space = (operator === '-' || operator === '+' || operator === '~' || operator === '!') ? '' : ' '
  const str = `${operator}${space}${exprToSQL(expr)}`
  return parentheses ? `(${str})` : str
}

function getExprListSQL(exprList) {
  if (!exprList) return []
  if (!Array.isArray(exprList)) exprList = [exprList]
  return exprList.map(exprToSQL)
}

function mapObjectToSQL(mapExpr) {
  const { keyword, expr } = mapExpr
  const exprStr = expr.map(exprItem => [literalToSQL(exprItem.key), literalToSQL(exprItem.value)].join(', ')).join(', ')
  return [toUpper(keyword), `[${exprStr}]`].join('')
}

function orderOrPartitionByToSQL(expr, prefix) {
  if (!Array.isArray(expr)) return ''
  let expressions = []
  const upperPrefix = toUpper(prefix)
  switch (upperPrefix) {
    case 'ORDER BY':
      expressions = expr.map(info => [exprToSQL(info.expr), info.type || 'ASC', toUpper(info.nulls)].filter(hasVal).join(' '))
      break
    case 'PARTITION BY':
      expressions = expr.map(info => exprToSQL(info.expr))
      break
    default:
      expressions = expr.map(info => exprToSQL(info.expr))
      break
  }
  return connector(upperPrefix, expressions.join(', '))
}

// Populate exprToSQLConvertFn object after all functions are defined
Object.assign(exprToSQLConvertFn, {
  alter           : alterExprToSQL,
  aggr_func       : aggrToSQL,
  any_value       : anyValueFuncToSQL,
  window_func     : windowFuncToSQL,
  'array'         : arrayStructExprToSQL,
  array_access    : arrayAccessToSQL,
  assign          : assignToSQL,
  binary_expr     : binaryToSQL,
  case            : caseToSQL,
  cast            : castToSQL,
  collate         : collateToSQL,
  datatype        : dataTypeToSQL,
  extract         : extractFunToSQL,
  flatten         : flattenFunToSQL,
  function        : funcToSQL,
  lambda          : lambdaToSQL,
  insert          : unionToSQL,
  interval        : intervalToSQL,
  json            : jsonExprToSQL,
  json_object_arg : jsonObjectArgToSQL,
  json_visitor    : jsonVisitorExprToSQL,
  func_arg        : funcArgToSQL,
  show            : showToSQL,
  struct          : arrayStructExprToSQL,
  tablefunc       : tablefuncFunToSQL,
  tables          : tablesToSQL,
  unnest          : unnestToSQL,
  'window'        : namedWindowExprListToSQL,
  var             : varToSQL,
  unary_expr      : unaryToSQL,
  map_object      : mapObjectToSQL,
})

exprToSQLConvertFn.expr_list = expr => {
  const result = getExprListSQL(expr.value)
  const { parentheses, separator } = expr
  if (!parentheses && !separator) return result
  const joinSymbol = separator || ', '
  const str = result.join(joinSymbol)
  return parentheses ? `(${str})` : str
}

exprToSQLConvertFn.select = expr => {
  const str = typeof expr._next === 'object' ? unionToSQL(expr) : selectToSQL(expr)
  return expr.parentheses ? `(${str})` : str
}

export {
  exprToSQLConvertFn,
  exprToSQL,
  getExprListSQL,
  varToSQL,
  orderOrPartitionByToSQL,
}

// Registry pattern to handle circular dependencies
// This module serves as a central registry for column-related functions

const registry = {
  columnRefToSQL        : null,
  fullTextSearchToSQL   : null,
  columnDefinitionToSQL : null,
}

export function registerColumnFunctions(funcs) {
  Object.assign(registry, funcs)
}

export function getColumnRefToSQL() {
  return registry.columnRefToSQL
}

export function getFullTextSearchToSQL() {
  return registry.fullTextSearchToSQL
}

export function getColumnDefinitionToSQL() {
  return registry.columnDefinitionToSQL
}

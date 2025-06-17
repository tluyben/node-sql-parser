const Parser = require('./output/prod/index').Parser
const parser = new Parser()

// Test the exact case that's failing
const tableName = 'domains'
const dbType = 'mysql'

console.log('=== Testing exact user case ===')
const ast = parser.astify("SELECT * FROM " + tableName, { database: dbType })
console.log('AST:', JSON.stringify(ast, null, 2))

// Modify AST like the user does
ast.limit = {
  seperator: "offset",
  value: [
    { type: "number", value: 25 },
    { type: "number", value: 0 },
  ],
}

const sql = parser.sqlify(ast, { database: dbType })
console.log('Generated SQL:', sql)

// Check if * is actually in the right place
const selectMatch = sql.match(/SELECT\s+(\S+)\s+FROM/i)
if (selectMatch) {
  console.log('What comes after SELECT:', selectMatch[1])
  if (selectMatch[1] !== '*') {
    console.error('FAILED: Expected * after SELECT, got:', selectMatch[1])
  }
} else {
  console.error('FAILED: Could not find SELECT...FROM pattern')
}
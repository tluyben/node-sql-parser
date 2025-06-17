const Parser = require('./output/prod/index').Parser;
const parser = new Parser();

// Test the extraction function
const extractStringValue = (value) => {
  console.log('extractStringValue called with:', JSON.stringify(value))
  console.log('typeof value:', typeof value)
  
  if (typeof value === 'string') {
    console.log('Returning string:', value)
    return value
  }
  
  if (typeof value === 'object' && value && value.expr) {
    console.log('Has expr:', JSON.stringify(value.expr))
    if (value.expr.value) {
      console.log('Returning expr.value:', value.expr.value)
      return value.expr.value
    }
  }
  
  console.log('Returning null')
  return null
}

// Test what the AST looks like for WHERE clause
const sql = "SELECT * FROM users WHERE id = 1";
const ast = parser.astify(sql, { database: 'postgresql' });

console.log('Testing extraction on column:');
const columnValue = extractStringValue(ast.where.left.column);
console.log('Final result:', columnValue);
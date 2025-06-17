const Parser = require('./output/prod/index').Parser;
const parser = new Parser();

console.log('Debugging PostgreSQL column references:');

// Test what the AST looks like for WHERE clause
const sql = "SELECT * FROM users WHERE id = 1";
const ast = parser.astify(sql, { database: 'postgresql' });

console.log('WHERE clause left side:');
console.log(JSON.stringify(ast.where.left, null, 2));

// Test conversion
console.log('\nConverting WHERE left side:');
const result = parser.exprToSQL(ast.where.left, { database: 'postgresql' });
console.log('Result:', result);
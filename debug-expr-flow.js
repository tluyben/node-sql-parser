const Parser = require('./output/prod/index').Parser;
const parser = new Parser();

// We need to rebuild first to get our changes
console.log('Building...');
require('child_process').execSync('npm run build', { stdio: 'inherit' });

console.log('Testing after rebuild...');

// Test what the AST looks like for WHERE clause
const sql = "SELECT * FROM users WHERE id = 1";
const ast = parser.astify(sql, { database: 'postgresql' });

console.log('Converting WHERE left side:');
const result = parser.exprToSQL(ast.where.left, { database: 'postgresql' });
console.log('Result:', result);
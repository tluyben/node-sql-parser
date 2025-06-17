const Parser = require('./output/prod/index').Parser;
const parser = new Parser();

console.log('Testing fulltext search:');

const sql = "SELECT MATCH (`label`) AGAINST (?) AS `score` FROM `TABLE` ORDER BY `score` DESC";
console.log('Original SQL:', sql);

try {
  const ast = parser.astify(sql, { database: 'mysql' });
  console.log('AST columns:', JSON.stringify(ast.columns, null, 2));
  
  const result = parser.sqlify(ast, { database: 'mysql' });
  console.log('Result SQL:', result);
  console.log('Match?:', sql === result);
} catch (e) {
  console.log('Error:', e.message);
}
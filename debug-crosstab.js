const Parser = require('./output/prod/index').Parser;
const parser = new Parser();

console.log('Testing crosstab function:');

const sql = `SELECT * FROM crosstab('select student, subject, evaluation_result from evaluations order by 1,2', $$VALUES ('t'::text), ('f'::text)$$) AS final_result(Student TEXT, Geography NUMERIC, History NUMERIC, Language NUMERIC, Maths NUMERIC, Music NUMERIC)`;
console.log('Original SQL:', sql);

try {
  const ast = parser.astify(sql, { database: 'postgresql' });
  console.log('AST from:', JSON.stringify(ast.from, null, 2));
  
  const result = parser.sqlify(ast, { database: 'postgresql' });
  console.log('Result SQL:', result);
  console.log('Match?:', sql === result);
} catch (e) {
  console.log('Error:', e.message);
}
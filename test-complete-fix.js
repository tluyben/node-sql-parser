const Parser = require('./output/prod/index').Parser;
const parser = new Parser();

console.log('Testing complete fix:');

// Test 1: SELECT * (the original issue)
console.log('\n1. Testing SELECT *:');
['mysql', 'postgresql', 'clickhouse', 'duckdb'].forEach(db => {
  try {
    const sql = "SELECT * FROM users";
    const ast = parser.astify(sql, { database: db });
    const result = parser.sqlify(ast, { database: db });
    console.log(`${db}: ${result} - Star: ${result.includes('*') ? '✓' : '✗'}`);
  } catch (e) {
    console.log(`${db}: Error - ${e.message}`);
  }
});

// Test 2: SELECT with WHERE (the broken tests)
console.log('\n2. Testing SELECT * with WHERE:');
['mysql', 'postgresql'].forEach(db => {
  try {
    const sql = "SELECT * FROM users WHERE id = 1";
    const ast = parser.astify(sql, { database: db });
    const result = parser.sqlify(ast, { database: db });
    console.log(`${db}: ${result} - Has 'id': ${result.includes('id') ? '✓' : '✗'}`);
  } catch (e) {
    console.log(`${db}: Error - ${e.message}`);
  }
});

// Test 3: SELECT specific columns
console.log('\n3. Testing SELECT specific columns:');
['mysql', 'postgresql'].forEach(db => {
  try {
    const sql = "SELECT id, name FROM users";
    const ast = parser.astify(sql, { database: db });
    const result = parser.sqlify(ast, { database: db });
    console.log(`${db}: ${result} - Has 'id' & 'name': ${result.includes('id') && result.includes('name') ? '✓' : '✗'}`);
  } catch (e) {
    console.log(`${db}: Error - ${e.message}`);
  }
});
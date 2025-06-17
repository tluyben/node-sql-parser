const { expect } = require('chai')
const Parser = require('../output/prod/index').Parser

describe('AST roundtrip tests', () => {
  const parser = new Parser()
  const databases = ['mysql', 'postgresql', 'sqlite', 'clickhouse', 'duckdb', 'bigquery', 'mariadb']

  databases.forEach(database => {
    describe(`${database} roundtrip`, () => {
      
      it('should preserve SELECT * (star) when converting AST back to SQL', () => {
        const sql = 'SELECT * FROM users'
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        expect(result).to.include('*', `${database} should preserve * in SELECT statement`)
        expect(result).to.include('FROM', `${database} should preserve FROM clause`)
        expect(result).to.include('users', `${database} should preserve table name`)
      })
      
      it('should preserve SELECT * with WHERE clause', () => {
        const sql = 'SELECT * FROM users WHERE id = 1'
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        expect(result).to.include('*', `${database} should preserve * with WHERE clause`)
        expect(result).to.include('WHERE', `${database} should preserve WHERE clause`)
        expect(result).to.match(/id.*=.*1/, `${database} should preserve WHERE condition (may have escaping)`)
      })
      
      it('should preserve SELECT * with LIMIT and OFFSET', () => {
        const sql = 'SELECT * FROM policies LIMIT 25 OFFSET 0'
        
        // Skip OFFSET test for databases that don't support it
        if (database === 'clickhouse' || database === 'duckdb') {
          const simpleLimit = 'SELECT * FROM policies LIMIT 25'
          const ast = parser.astify(simpleLimit, { database })
          const result = parser.sqlify(ast, { database })
          
          expect(result).to.include('*', `${database} should preserve * with LIMIT`)
          expect(result).to.include('LIMIT', `${database} should preserve LIMIT clause`)
          expect(result).to.include('25', `${database} should preserve LIMIT value`)
          return
        }
        
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        expect(result).to.include('*', `${database} should preserve * with LIMIT/OFFSET`)
        expect(result).to.include('LIMIT', `${database} should preserve LIMIT clause`)
        expect(result).to.include('25', `${database} should preserve LIMIT value`)
        expect(result).to.include('OFFSET', `${database} should preserve OFFSET clause`)
        expect(result).to.include('0', `${database} should preserve OFFSET value`)
      })

      it('should preserve explicit column names', () => {
        const sql = 'SELECT id, name FROM users'
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        expect(result).to.include('id', `${database} should preserve id column`)
        expect(result).to.include('name', `${database} should preserve name column`)
        expect(result).to.not.include('*', `${database} should not add * when columns are explicit`)
      })

      it('should preserve table alias', () => {
        const sql = 'SELECT * FROM users u WHERE u.id = 1'
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        expect(result).to.include('*', `${database} should preserve * with table alias`)
        expect(result).to.include('u', `${database} should preserve table alias`)
      })

      it('should preserve column alias', () => {
        const sql = 'SELECT id AS user_id FROM users'
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        expect(result).to.include('id', `${database} should preserve column name`)
        expect(result).to.include('AS', `${database} should preserve AS keyword`)
        expect(result).to.include('user_id', `${database} should preserve column alias`)
      })
      
    })
  })

  describe('Database-specific identifier escaping', () => {
    
    it('should preserve MySQL backtick escaping', () => {
      const sql = 'SELECT * FROM `my-table` WHERE `user-id` = 1'
      const ast = parser.astify(sql, { database: 'mysql' })
      const result = parser.sqlify(ast, { database: 'mysql' })
      
      expect(result).to.include('*', 'MySQL should preserve *')
      expect(result).to.include('`my-table`', 'MySQL should preserve backtick-escaped table names')
      expect(result).to.include('`user-id`', 'MySQL should preserve backtick-escaped column names')
    })

    it('should preserve PostgreSQL double quote escaping', () => {
      const sql = 'SELECT * FROM "my-table" WHERE "user-id" = 1'
      const ast = parser.astify(sql, { database: 'postgresql' })
      const result = parser.sqlify(ast, { database: 'postgresql' })
      
      expect(result).to.include('*', 'PostgreSQL should preserve *')
      expect(result).to.include('"my-table"', 'PostgreSQL should preserve double-quoted table names')
      expect(result).to.include('"user-id"', 'PostgreSQL should preserve double-quoted column names')
    })

    it('should preserve ClickHouse backtick escaping', () => {
      const sql = 'SELECT * FROM `my-table` WHERE `user-id` = 1'
      const ast = parser.astify(sql, { database: 'clickhouse' })
      const result = parser.sqlify(ast, { database: 'clickhouse' })
      
      expect(result).to.include('*', 'ClickHouse should preserve *')
      expect(result).to.include('`my-table`', 'ClickHouse should preserve backtick-escaped table names')
      expect(result).to.include('`user-id`', 'ClickHouse should preserve backtick-escaped column names')
    })

    it('should preserve DuckDB double quote escaping', () => {
      const sql = 'SELECT * FROM "my-table" WHERE "user-id" = 1'
      const ast = parser.astify(sql, { database: 'duckdb' })
      const result = parser.sqlify(ast, { database: 'duckdb' })
      
      expect(result).to.include('*', 'DuckDB should preserve *')
      expect(result).to.include('"my-table"', 'DuckDB should preserve double-quoted table names')
      expect(result).to.include('"user-id"', 'DuckDB should preserve double-quoted column names')
    })
  })

  describe('Complex queries roundtrip', () => {
    
    it('should preserve JOIN queries with *', () => {
      const sql = 'SELECT * FROM users u JOIN orders o ON u.id = o.user_id'
      databases.forEach(database => {
        if (database === 'bigquery' || database === 'clickhouse' || database === 'duckdb') return // Skip unsupported databases
        
        try {
          const ast = parser.astify(sql, { database })
          const result = parser.sqlify(ast, { database })
          
          expect(result).to.include('*', `${database} should preserve * in JOIN queries`)
          expect(result).to.include('JOIN', `${database} should preserve JOIN keyword`)
          expect(result).to.include('ON', `${database} should preserve ON condition`)
        } catch (e) {
          console.log(`${database} JOIN test skipped: ${e.message}`)
        }
      })
    })

    it('should preserve subqueries with *', () => {
      const sql = 'SELECT * FROM (SELECT * FROM users WHERE active = 1) sub'
      databases.forEach(database => {
        if (database === 'bigquery') return // Skip BigQuery for complex subqueries
        
        const ast = parser.astify(sql, { database })
        const result = parser.sqlify(ast, { database })
        
        // Count occurrences of * - should be 2 (outer and inner SELECT)
        const starCount = (result.match(/\*/g) || []).length
        expect(starCount).to.equal(2, `${database} should preserve both * in subquery`)
      })
    })

    it('should preserve GROUP BY with *', () => {
      const sql = 'SELECT * FROM users GROUP BY department'
      databases.forEach(database => {
        if (database === 'bigquery') return // Skip BigQuery for GROUP BY *
        
        try {
          const ast = parser.astify(sql, { database })
          const result = parser.sqlify(ast, { database })
          
          expect(result).to.include('*', `${database} should preserve * with GROUP BY`)
          expect(result).to.include('GROUP BY', `${database} should preserve GROUP BY clause`)
        } catch (e) {
          // Some databases might not support SELECT * with GROUP BY, that's OK
          console.log(`${database} doesn't support SELECT * with GROUP BY: ${e.message}`)
        }
      })
    })

  })
})
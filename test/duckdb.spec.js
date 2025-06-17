const { expect } = require('chai');
const Parser = require('../src/parser').default

describe('DuckDB', () => {
  const parser = new Parser();
  function getParsedSql(sql, opt) {
    const ast = parser.astify(sql, opt);
    return parser.sqlify(ast, opt);
  }
  const duckdb = { database: 'duckdb' }

  describe('data types', () => {
    const SQL_LIST = [
      {
        title: 'BOOLEAN data type',
        sql: [
          'CREATE TABLE test (flag BOOLEAN)',
          'CREATE TABLE "test" ("flag" BOOLEAN)'
        ]
      },
      {
        title: 'BOOL data type alias',
        sql: [
          'CREATE TABLE test (flag BOOL)',
          'CREATE TABLE "test" ("flag" BOOL)'
        ]
      },
      {
        title: 'LOGICAL data type alias',
        sql: [
          'CREATE TABLE test (flag LOGICAL)',
          'CREATE TABLE "test" ("flag" LOGICAL)'
        ]
      },
      {
        title: 'Integer types',
        sql: [
          'CREATE TABLE test (tiny TINYINT, small SMALLINT, regular INTEGER, big BIGINT, huge HUGEINT)',
          'CREATE TABLE "test" ("tiny" TINYINT, "small" SMALLINT, "regular" INTEGER, "big" BIGINT, "huge" HUGEINT)'
        ]
      },
      {
        title: 'Float types',
        sql: [
          'CREATE TABLE test (f FLOAT, d DOUBLE)',
          'CREATE TABLE "test" ("f" FLOAT, "d" DOUBLE)'
        ]
      },
      {
        title: 'Text types',
        sql: [
          'CREATE TABLE test (name VARCHAR)',
          'CREATE TABLE "test" ("name" VARCHAR)'
        ]
      },
      {
        title: 'VARCHAR with length',
        sql: [
          'CREATE TABLE test (name VARCHAR(255))',
          'CREATE TABLE "test" ("name" VARCHAR(255))'
        ]
      },
      {
        title: 'UUID data type',
        sql: [
          'CREATE TABLE test (id UUID)',
          'CREATE TABLE "test" ("id" UUID)'
        ]
      },
      {
        title: 'Date and time types',
        sql: [
          'CREATE TABLE test (d DATE, t TIME, ts TIMESTAMP)',
          'CREATE TABLE "test" ("d" DATE, "t" TIME, "ts" TIMESTAMP)'
        ]
      },
      {
        title: 'Binary types',
        sql: [
          'CREATE TABLE test (data BLOB)',
          'CREATE TABLE "test" ("data" BLOB)'
        ]
      },
      {
        title: 'BYTEA binary type',
        sql: [
          'CREATE TABLE test (data BYTEA)',
          'CREATE TABLE "test" ("data" BYTEA)'
        ]
      },
      {
        title: 'JSON data type',
        sql: [
          'CREATE TABLE test (data JSON)',
          'CREATE TABLE "test" ("data" JSON)'
        ]
      },
      {
        title: 'DECIMAL with precision and scale',
        sql: [
          'CREATE TABLE test (price DECIMAL(10, 2))',
          'CREATE TABLE "test" ("price" DECIMAL(10, 2))'
        ]
      },
      {
        title: 'INTERVAL data type',
        sql: [
          'CREATE TABLE test (duration INTERVAL)',
          'CREATE TABLE "test" ("duration" INTERVAL)'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('nested data types', () => {
    const SQL_LIST = [
      {
        title: 'LIST data type',
        sql: [
          'CREATE TABLE test (tags LIST(VARCHAR))',
          'CREATE TABLE "test" ("tags" LIST(VARCHAR))'
        ]
      },
      {
        title: 'ARRAY data type',
        sql: [
          'CREATE TABLE test (numbers ARRAY(INTEGER))',
          'CREATE TABLE "test" ("numbers" ARRAY(INTEGER))'
        ]
      },
      {
        title: 'Nested ARRAY',
        sql: [
          'CREATE TABLE test (matrix ARRAY(ARRAY(INTEGER)))',
          'CREATE TABLE "test" ("matrix" ARRAY(ARRAY(INTEGER)))'
        ]
      },
      {
        title: 'MAP data type',
        sql: [
          'CREATE TABLE test (metadata MAP(VARCHAR, VARCHAR))',
          'CREATE TABLE "test" ("metadata" MAP(VARCHAR, VARCHAR))'
        ]
      },
      {
        title: 'STRUCT data type',
        sql: [
          'CREATE TABLE test (person STRUCT(name VARCHAR, age INTEGER))',
          'CREATE TABLE "test" ("person" STRUCT("name" VARCHAR, "age" INTEGER))'
        ]
      },
      {
        title: 'UNION data type',
        sql: [
          'CREATE TABLE test (value UNION(INTEGER, VARCHAR))',
          'CREATE TABLE "test" ("value" UNION(INTEGER, VARCHAR))'
        ]
      },
      {
        title: 'Complex nested STRUCT',
        sql: [
          'CREATE TABLE test (data STRUCT(id INTEGER, metadata MAP(VARCHAR, VARCHAR)))',
          'CREATE TABLE "test" ("data" STRUCT("id" INTEGER, "metadata" MAP(VARCHAR, VARCHAR)))'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('create table', () => {
    const SQL_LIST = [
      {
        title: 'basic create table',
        sql: [
          'CREATE TABLE test (id INTEGER, name VARCHAR)',
          'CREATE TABLE "test" ("id" INTEGER, "name" VARCHAR)'
        ]
      },
      {
        title: 'create table if not exists',
        sql: [
          'CREATE TABLE IF NOT EXISTS test (id INTEGER)',
          'CREATE TABLE IF NOT EXISTS "test" ("id" INTEGER)'
        ]
      },
      {
        title: 'create table with defaults',
        sql: [
          "CREATE TABLE test (id INTEGER DEFAULT 1, name VARCHAR DEFAULT 'test')",
          "CREATE TABLE \"test\" (\"id\" INTEGER DEFAULT 1, \"name\" VARCHAR DEFAULT 'test')"
        ]
      },
      {
        title: 'create table with NOT NULL',
        sql: [
          'CREATE TABLE test (id INTEGER NOT NULL, name VARCHAR)',
          'CREATE TABLE "test" ("id" INTEGER NOT NULL, "name" VARCHAR)'
        ]
      },
      {
        title: 'create table with comments',
        sql: [
          "CREATE TABLE test (id INTEGER COMMENT 'primary key')",
          "CREATE TABLE \"test\" (\"id\" INTEGER COMMENT 'primary key')"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('select queries', () => {
    const SQL_LIST = [
      {
        title: 'basic select',
        sql: [
          'SELECT * FROM users',
          'SELECT * FROM "users"'
        ]
      },
      {
        title: 'select with specific columns',
        sql: [
          'SELECT id, name FROM users',
          'SELECT "id", "name" FROM "users"'
        ]
      },
      {
        title: 'select with WHERE clause',
        sql: [
          "SELECT * FROM users WHERE name = 'John'",
          "SELECT * FROM \"users\" WHERE \"name\" = 'John'"
        ]
      },
      {
        title: 'select with DISTINCT',
        sql: [
          'SELECT DISTINCT name FROM users',
          'SELECT DISTINCT "name" FROM "users"'
        ]
      },
      {
        title: 'select with GROUP BY',
        sql: [
          'SELECT status, count() FROM orders GROUP BY status',
          'SELECT "status", count() FROM "orders" GROUP BY "status"'
        ]
      },
      {
        title: 'select with ORDER BY',
        sql: [
          'SELECT * FROM users ORDER BY name ASC',
          'SELECT * FROM "users" ORDER BY "name" ASC'
        ]
      },
      {
        title: 'select with LIMIT',
        sql: [
          'SELECT * FROM users LIMIT 10',
          'SELECT * FROM "users" LIMIT 10'
        ]
      },
      {
        title: 'select with HAVING',
        sql: [
          'SELECT status, count() FROM orders GROUP BY status HAVING count() > 5',
          'SELECT "status", count() FROM "orders" GROUP BY "status" HAVING count() > 5'
        ]
      },
      {
        title: 'select with aliases',
        sql: [
          'SELECT name AS full_name FROM users',
          'SELECT "name" AS full_name FROM "users"'
        ]
      },
      {
        title: 'select with table aliases',
        sql: [
          'SELECT u.name FROM users u',
          'SELECT "u"."name" FROM "users" "u"'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('insert queries', () => {
    const SQL_LIST = [
      {
        title: 'basic insert',
        sql: [
          "INSERT INTO users (id, name) VALUES (1, 'John')",
          "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'John')"
        ]
      },
      {
        title: 'insert without column list',
        sql: [
          "INSERT INTO users VALUES (1, 'John')",
          "INSERT INTO \"users\" VALUES (1, 'John')"
        ]
      },
      {
        title: 'insert multiple rows',
        sql: [
          "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')",
          "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'John'), (2, 'Jane')"
        ]
      },
      {
        title: 'insert with nested data',
        sql: [
          "INSERT INTO products (id, metadata) VALUES (1, {'name': 'Product 1', 'price': 100})",
          "INSERT INTO \"products\" (\"id\", \"metadata\") VALUES (1, {'name': 'Product 1', 'price': 100})"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('update queries', () => {
    const SQL_LIST = [
      {
        title: 'basic update',
        sql: [
          "UPDATE users SET name = 'John Updated' WHERE id = 1",
          "UPDATE \"users\" SET \"name\" = 'John Updated' WHERE \"id\" = 1"
        ]
      },
      {
        title: 'update multiple columns',
        sql: [
          "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1",
          "UPDATE \"users\" SET \"name\" = 'John', \"email\" = 'john@example.com' WHERE \"id\" = 1"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('delete queries', () => {
    const SQL_LIST = [
      {
        title: 'basic delete',
        sql: [
          'DELETE FROM users WHERE id = 1',
          'DELETE FROM "users" WHERE "id" = 1'
        ]
      },
      {
        title: 'delete with complex condition',
        sql: [
          "DELETE FROM users WHERE name = 'John' AND age > 25",
          "DELETE FROM \"users\" WHERE \"name\" = 'John' AND \"age\" > 25"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('drop statements', () => {
    const SQL_LIST = [
      {
        title: 'drop table',
        sql: [
          'DROP TABLE users',
          'DROP TABLE "users"'
        ]
      },
      {
        title: 'drop table if exists',
        sql: [
          'DROP TABLE IF EXISTS users',
          'DROP TABLE IF EXISTS "users"'
        ]
      },
      {
        title: 'drop database',
        sql: [
          'DROP DATABASE test_db',
          'DROP DATABASE "test_db"'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('show statements', () => {
    const SQL_LIST = [
      {
        title: 'show databases',
        sql: [
          'SHOW DATABASES',
          'SHOW DATABASES'
        ]
      },
      {
        title: 'show tables',
        sql: [
          'SHOW TABLES',
          'SHOW TABLES'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('DuckDB-specific features', () => {
    const SQL_LIST = [
      {
        title: 'create table with complex nested types',
        sql: [
          'CREATE TABLE events (id UUID, user_data STRUCT(name VARCHAR, contacts LIST(VARCHAR)), metadata MAP(VARCHAR, JSON))',
          'CREATE TABLE "events" ("id" UUID, "user_data" STRUCT("name" VARCHAR, "contacts" LIST(VARCHAR)), "metadata" MAP(VARCHAR, JSON))'
        ]
      },
      {
        title: 'HUGEINT data type',
        sql: [
          'CREATE TABLE test (big_number HUGEINT)',
          'CREATE TABLE "test" ("big_number" HUGEINT)'
        ]
      },
      {
        title: 'complex nested STRUCT with multiple fields',
        sql: [
          'CREATE TABLE analytics (event_id UUID, user_profile STRUCT(id INTEGER, name VARCHAR, preferences MAP(VARCHAR, BOOLEAN)))',
          'CREATE TABLE "analytics" ("event_id" UUID, "user_profile" STRUCT("id" INTEGER, "name" VARCHAR, "preferences" MAP(VARCHAR, BOOLEAN)))'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], duckdb)).to.equal(sql[1])
      })
    })
  })

  describe('AST validation', () => {
    it('should parse BOOLEAN data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (flag BOOLEAN)'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('BOOLEAN')
    })

    it('should parse LIST data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (tags LIST(VARCHAR))'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('LIST')
      expect(ast.create_definitions[0].definition.elementType.dataType).to.equal('VARCHAR')
    })

    it('should parse MAP data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (metadata MAP(VARCHAR, INTEGER))'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('MAP')
      expect(ast.create_definitions[0].definition.keyType.dataType).to.equal('VARCHAR')
      expect(ast.create_definitions[0].definition.valueType.dataType).to.equal('INTEGER')
    })

    it('should parse STRUCT data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (person STRUCT(name VARCHAR, age INTEGER))'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('STRUCT')
      expect(ast.create_definitions[0].definition.fields).to.have.length(2)
      expect(ast.create_definitions[0].definition.fields[0].name).to.equal('name')
      expect(ast.create_definitions[0].definition.fields[0].type.dataType).to.equal('VARCHAR')
      expect(ast.create_definitions[0].definition.fields[1].name).to.equal('age')
      expect(ast.create_definitions[0].definition.fields[1].type.dataType).to.equal('INTEGER')
    })

    it('should parse UNION data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (value UNION(INTEGER, VARCHAR))'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('UNION')
      expect(ast.create_definitions[0].definition.memberTypes).to.have.length(2)
      expect(ast.create_definitions[0].definition.memberTypes[0].dataType).to.equal('INTEGER')
      expect(ast.create_definitions[0].definition.memberTypes[1].dataType).to.equal('VARCHAR')
    })

    it('should parse UUID data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (id UUID)'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('UUID')
    })

    it('should parse JSON data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (data JSON)'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('JSON')
    })

    it('should parse HUGEINT data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (big_num HUGEINT)'
      const ast = parser.astify(sql, duckdb)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('HUGEINT')
    })
  })

  describe('error handling', () => {
    it('should handle DuckDB-specific syntax errors gracefully', () => {
      const sql = 'CREATE TABLE test (name VARCHAR INVALID_CONSTRAINT)'
      
      expect(() => {
        parser.astify(sql, duckdb)
      }).to.throw()
    })

    it('should handle invalid nested type syntax', () => {
      const sql = 'CREATE TABLE test (data LIST(INVALID_TYPE))'
      
      expect(() => {
        parser.astify(sql, duckdb)
      }).to.throw()
    })
  })
})
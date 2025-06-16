const { expect } = require('chai');
const Parser = require('../src/parser').default

describe('ClickHouse', () => {
  const parser = new Parser();
  function getParsedSql(sql, opt) {
    const ast = parser.astify(sql, opt);
    return parser.sqlify(ast, opt);
  }
  const clickhouse = { database: 'clickhouse' }

  describe('data types', () => {
    const SQL_LIST = [
      {
        title: 'String data type',
        sql: [
          'CREATE TABLE test (name String)',
          'CREATE TABLE `test` (`name` String)'
        ]
      },
      {
        title: 'String with default',
        sql: [
          "CREATE TABLE test (name String DEFAULT 'default_value')",
          "CREATE TABLE `test` (`name` String DEFAULT 'default_value')"
        ]
      },
      {
        title: 'ClickHouse integer types',
        sql: [
          'CREATE TABLE test (col1 INT8, col2 INT16, col3 INT32, col4 INT64)',
          'CREATE TABLE `test` (`col1` INT8, `col2` INT16, `col3` INT32, `col4` INT64)'
        ]
      },
      {
        title: 'ClickHouse unsigned integer types',
        sql: [
          'CREATE TABLE test (col1 UINT8, col2 UINT16, col3 UINT32, col4 UINT64)',
          'CREATE TABLE `test` (`col1` UINT8, `col2` UINT16, `col3` UINT32, `col4` UINT64)'
        ]
      },
      {
        title: 'ClickHouse float types',
        sql: [
          'CREATE TABLE test (col1 FLOAT32, col2 FLOAT64)',
          'CREATE TABLE `test` (`col1` FLOAT32, `col2` FLOAT64)'
        ]
      },
      {
        title: 'UUID data type',
        sql: [
          'CREATE TABLE test (id UUID)',
          'CREATE TABLE `test` (`id` UUID)'
        ]
      },
      {
        title: 'DATETIME64 with precision',
        sql: [
          'CREATE TABLE test (`timestamp` DATETIME64(3))',
          'CREATE TABLE `test` (`timestamp` DATETIME64(3))'
        ]
      },
      {
        title: 'Array data type',
        sql: [
          'CREATE TABLE test (tags ARRAY(String))',
          'CREATE TABLE `test` (`tags` ARRAY(String))'
        ]
      },
      {
        title: 'Nested Array data type',
        sql: [
          'CREATE TABLE test (matrix ARRAY(ARRAY(INT32)))',
          'CREATE TABLE `test` (`matrix` ARRAY(ARRAY(INT32)))'
        ]
      },
      {
        title: 'Tuple data type',
        sql: [
          'CREATE TABLE test (coord TUPLE(FLOAT32, FLOAT32))',
          'CREATE TABLE `test` (`coord` TUPLE(FLOAT32, FLOAT32))'
        ]
      },
      {
        title: 'Map data type',
        sql: [
          'CREATE TABLE test (attrs MAP(String, String))',
          'CREATE TABLE `test` (`attrs` MAP(String, String))'
        ]
      },
      {
        title: 'Nullable data type',
        sql: [
          'CREATE TABLE test (optional_value NULLABLE(INT32))',
          'CREATE TABLE `test` (`optional_value` NULLABLE(INT32))'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('create table', () => {
    const SQL_LIST = [
      {
        title: 'create table with engine',
        sql: [
          'CREATE TABLE test (id INT32, name String) ENGINE = MergeTree',
          'CREATE TABLE `test` (`id` INT32, `name` String) ENGINE = MergeTree'
        ]
      },
      {
        title: 'create table if not exists',
        sql: [
          'CREATE TABLE IF NOT EXISTS test (id INT32)',
          'CREATE TABLE IF NOT EXISTS `test` (`id` INT32)'
        ]
      },
      {
        title: 'create table with comments',
        sql: [
          "CREATE TABLE test (id INT32 COMMENT 'primary key', name String)",
          "CREATE TABLE `test` (`id` INT32 COMMENT 'primary key', `name` String)"
        ]
      },
      {
        title: 'create table with not null constraint',
        sql: [
          'CREATE TABLE test (id INT32 NOT NULL, name String)',
          'CREATE TABLE `test` (`id` INT32 NOT NULL, `name` String)'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('select queries', () => {
    const SQL_LIST = [
      {
        title: 'basic select',
        sql: [
          'SELECT * FROM users',
          'SELECT * FROM `users`'
        ]
      },
      {
        title: 'select with String column',
        sql: [
          'SELECT name FROM users WHERE name = \'John\'',
          'SELECT `name` FROM `users` WHERE `name` = \'John\''
        ]
      },
      {
        title: 'select with ClickHouse functions',
        sql: [
          'SELECT toString(id), length(name) FROM users',
          'SELECT toString(`id`), length(`name`) FROM `users`'
        ]
      },
      {
        title: 'select with array functions',
        sql: [
          'SELECT arrayElement(tags, 1) FROM articles',
          'SELECT arrayElement(`tags`, 1) FROM `articles`'
        ]
      },
      {
        title: 'select with group by',
        sql: [
          'SELECT status, count() FROM orders GROUP BY status',
          'SELECT `status`, count() FROM `orders` GROUP BY `status`'
        ]
      },
      {
        title: 'select with limit',
        sql: [
          'SELECT * FROM users LIMIT 10',
          'SELECT * FROM `users` LIMIT 10'
        ]
      },
      {
        title: 'select with order by',
        sql: [
          'SELECT * FROM users ORDER BY created_at DESC',
          'SELECT * FROM `users` ORDER BY `created_at` DESC'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('insert queries', () => {
    const SQL_LIST = [
      {
        title: 'basic insert',
        sql: [
          "INSERT INTO users (id, name) VALUES (1, 'John')",
          "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John')"
        ]
      },
      {
        title: 'insert with String values',
        sql: [
          "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')",
          "INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@example.com')"
        ]
      },
      {
        title: 'insert multiple rows',
        sql: [
          "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')",
          "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John'), (2, 'Jane')"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('update queries', () => {
    const SQL_LIST = [
      {
        title: 'basic update',
        sql: [
          "UPDATE users SET name = 'John Updated' WHERE id = 1",
          "UPDATE `users` SET `name` = 'John Updated' WHERE `id` = 1"
        ]
      },
      {
        title: 'update with multiple columns',
        sql: [
          "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1",
          "UPDATE `users` SET `name` = 'John', `email` = 'john@example.com' WHERE `id` = 1"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('delete queries', () => {
    const SQL_LIST = [
      {
        title: 'basic delete',
        sql: [
          'DELETE FROM users WHERE id = 1',
          'DELETE FROM `users` WHERE `id` = 1'
        ]
      },
      {
        title: 'delete with string condition',
        sql: [
          "DELETE FROM users WHERE name = 'John'",
          "DELETE FROM `users` WHERE `name` = 'John'"
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('drop statements', () => {
    const SQL_LIST = [
      {
        title: 'drop table',
        sql: [
          'DROP TABLE users',
          'DROP TABLE `users`'
        ]
      },
      {
        title: 'drop table if exists',
        sql: [
          'DROP TABLE IF EXISTS users',
          'DROP TABLE IF EXISTS `users`'
        ]
      },
      {
        title: 'drop database',
        sql: [
          'DROP DATABASE test_db',
          'DROP DATABASE `test_db`'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
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
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('complex ClickHouse scenarios', () => {
    const SQL_LIST = [
      {
        title: 'create table with complex types',
        sql: [
          'CREATE TABLE events (id UUID, user_id INT64, event_data MAP(String, String), tags ARRAY(String), `timestamp` DATETIME64(3)) ENGINE = MergeTree',
          'CREATE TABLE `events` (`id` UUID, `user_id` INT64, `event_data` MAP(String, String), `tags` ARRAY(String), `timestamp` DATETIME64(3)) ENGINE = MergeTree'
        ]
      },
      {
        title: 'select with array and map operations',
        sql: [
          "SELECT id, arrayElement(tags, 1) as first_tag, event_data['action'] as action FROM events",
          "SELECT `id`, arrayElement(`tags`, 1) AS first_tag, event_data['action'] AS action FROM `events`"
        ]
      },
      {
        title: 'nullable columns',
        sql: [
          'CREATE TABLE users (id INT64, name String, email NULLABLE(String))',
          'CREATE TABLE `users` (`id` INT64, `name` String, `email` NULLABLE(String))'
        ]
      }
    ]

    SQL_LIST.forEach(sqlInfo => {
      const { title, sql } = sqlInfo
      it(`should parse ${title}`, () => {
        expect(getParsedSql(sql[0], clickhouse)).to.equal(sql[1])
      })
    })
  })

  describe('AST validation', () => {
    it('should parse String data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (name String)'
      const ast = parser.astify(sql, clickhouse)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('STRING')
    })

    it('should parse Array(String) data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (tags ARRAY(String))'
      const ast = parser.astify(sql, clickhouse)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('ARRAY')
      expect(ast.create_definitions[0].definition.elementType.dataType).to.equal('STRING')
    })

    it('should parse Map data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (attrs MAP(String, INT32))'
      const ast = parser.astify(sql, clickhouse)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('MAP')
      expect(ast.create_definitions[0].definition.keyType.dataType).to.equal('STRING')
      expect(ast.create_definitions[0].definition.valueType.dataType).to.equal('INT32')
    })

    it('should parse Nullable data type correctly in AST', () => {
      const sql = 'CREATE TABLE test (optional_value NULLABLE(INT32))'
      const ast = parser.astify(sql, clickhouse)
      
      expect(ast.create_definitions[0].definition.dataType).to.equal('NULLABLE')
      expect(ast.create_definitions[0].definition.innerType.dataType).to.equal('INT32')
    })
  })

  describe('error handling', () => {
    it('should handle ClickHouse-specific syntax errors gracefully', () => {
      const sql = 'CREATE TABLE test (name String INVALID_CONSTRAINT)'
      
      expect(() => {
        parser.astify(sql, clickhouse)
      }).to.throw()
    })
  })
})
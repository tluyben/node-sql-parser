{
  const reservedMap = {
    'ALTER': true,
    'ALL': true,
    'ADD': true,
    'AND': true,
    'ARRAY': true,
    'AS': true,
    'ASC': true,

    'BETWEEN': true,
    'BY': true,
    'BOOLEAN': true,
    'BOOL': true,
    'BIGINT': true,
    'BLOB': true,
    'BYTEA': true,
    'BINARY': true,

    'CASE': true,
    'CHAR': true,
    'CHECK': true,
    'COLLATE': true,
    'COLUMN': true,
    'CREATE': true,
    'CROSS': true,
    'CURRENT_DATE': true,
    'CURRENT_TIME': true,
    'CURRENT_TIMESTAMP': true,

    'DATABASE': true,
    'DATABASES': true,
    'DATE': true,
    'DATETIME': true,
    'DECIMAL': true,
    'DEFAULT': true,
    'DELETE': true,
    'DESC': true,
    'DESCRIBE': true,
    'DISTINCT': true,
    'DROP': true,
    'DOUBLE': true,

    'ELSE': true,
    'END': true,
    'ENUM': true,
    'EXISTS': true,
    'EXPLAIN': true,

    'FALSE': true,
    'FLOAT': true,
    'FROM': true,
    'FULL': true,
    'FUNCTION': true,

    'GROUP': true,

    'HAVING': true,
    'HUGEINT': true,

    'IF': true,
    'IN': true,
    'INDEX': true,
    'INNER': true,
    'INSERT': true,
    'INT': true,
    'INTEGER': true,
    'INTERVAL': true,
    'INTO': true,
    'IS': true,

    'JOIN': true,
    'JSON': true,

    'KEY': true,

    'LEFT': true,
    'LIKE': true,
    'LIMIT': true,
    'LIST': true,
    'LOGICAL': true,

    'MAP': true,

    'NOT': true,
    'NULL': true,

    'ON': true,
    'OR': true,
    'ORDER': true,
    'OUTER': true,

    'PRIMARY': true,

    'RIGHT': true,

    'SELECT': true,
    'SET': true,
    'SHOW': true,
    'SMALLINT': true,
    'STRING': true,
    'STRUCT': true,

    'TABLE': true,
    'THEN': true,
    'TIME': true,
    'TIMESTAMP': true,
    'TINYINT': true,
    'TRUE': true,
    'TYPE': true,

    'UNION': true,
    'UPDATE': true,
    'USE': true,
    'USING': true,
    'UUID': true,

    'VALUES': true,
    'VARCHAR': true,
    'VARBINARY': true,

    'WHEN': true,
    'WHERE': true,
    'WITH': true,
  }

  let varList = [];
  const tableList = new Set();
  const columnList = new Set();
  const IGNORE_TAGS = ['OrReplace', 'IfNotExists', 'IfExists', 'Recursive']

  function createUnaryExpr(op, e) {
    return {
      type: 'unary_expr',
      operator: op,
      expr: e
    };
  }

  function createBinaryExpr(op, left, right) {
    return {
      type: 'binary_expr',
      operator: op,
      left: left,
      right: right
    };
  }

  function createList(head, tail, pos) {
    const result = [head];
    for (let i = 0; i < tail.length; i++) {
      result.push(tail[i][pos]);
    }
    return result;
  }

  function createExprList(head, tail, pos) {
    const result = [head];
    for (let i = 0; i < tail.length; i++) {
      result.push(tail[i][pos || 3]);
    }
    return result;
  }

  function createExprListWithParentheses(head, tail, pos) {
    const result = [head];
    for (let i = 0; i < tail.length; i++) {
      result.push(tail[i][pos || 3]);
    }
    return {
      type: 'expr_list',
      value: result,
      parentheses: true
    };
  }

  function columnListTableAlias(columnList) {
    const newColumnsList = [];
    for(let column of columnList) {
      if(column === '*') {
        newColumnsList.push(column);
        continue;
      }
      const columnInfo = column.split('::');
      if(columnInfo && columnInfo[1]) {
        if(columnInfo[1] !== 'null') newColumnsList.push(columnInfo[1]);
      }
    }
    return Array.from(new Set(newColumnsList));
  }

  function commonKeywordFunc(kw) {
    return {
      type: 'function',
      name: kw
    }
  }

  function literalToSQL(literal) {
    const { type } = literal
    let { value } = literal
    switch (type) {
      case 'number':
        return value
      case 'string':
        return `'${value}'`
      case 'bool':
        return value ? 'TRUE' : 'FALSE'
      case 'null':
        return 'NULL'
      case 'star':
        return '*'
      default:
        return value
    }
  }

  function identifierToSql(ident, isDual) {
    const val = ident.value || ident
    return `"${val}"`
  }

  function commonOptionValue(head, tail) {
    const headUpperCase = head && head.toUpperCase && head.toUpperCase()
    if (headUpperCase === 'TRUE') return { type: 'bool', value: true }
    if (headUpperCase === 'FALSE') return { type: 'bool', value: false }
    return {
      type: 'origin',
      value: (tail && tail.length > 0) ? `${head} ${tail.join(' ')}` : head
    }
  }

  function setParserOpt(opt) {
    if (varList && varList.length > 0) {
      for(let i = 0; i < varList.length; i++) {
        const varInfo = varList[i]
        varInfo.db = opt && opt.database
        varInfo.table = opt && opt.table
      }
    }
  }

  const cacheKey = 'duckdb'
  function getParserOpt() {
    return {
      database: cacheKey,
      type: cacheKey,
    }
  }
}

start
  = crud_stmt

crud_stmt
  = union_stmt
  / update_stmt
  / replace_insert_stmt
  / insert_stmt
  / delete_stmt
  / cmd_stmt

cmd_stmt
  = show_stmt
  / desc_stmt
  / use_stmt
  / create_stmt
  / drop_stmt
  / set_stmt

show_stmt
  = KW_SHOW __ KW_DATABASES { 
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'show',
        keyword: 'databases'
      }
    };
  }
  / KW_SHOW __ KW_TABLES __ f:(KW_FROM __ ident)? { 
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'show',
        keyword: 'tables',
        from: f ? f[2] : null
      }
    };
  }

desc_stmt
  = (KW_DESCRIBE / KW_DESC) __ t:table_name {
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'desc',
        table: t
      }
    };
  }

use_stmt
  = KW_USE __ d:ident {
    tableList.add(`use::${d}::null`);
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'use',
        db: d
      }
    };
  }

create_stmt
  = create_table_stmt
  / create_database_stmt

create_database_stmt
  = a:KW_CREATE __ KW_DATABASE __ ife:if_not_exists_stmt? __ t:ident {
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: a.toLowerCase(),
        keyword: 'database',
        if_not_exists: ife,
        database: t
      }
    };
  }

create_table_stmt
  = a:KW_CREATE __ KW_TABLE __ ife:if_not_exists_stmt? __ t:table_name __ LPAREN __ cd:create_column_definition __ RPAREN {
    if (t) tableList.add(`${a}::${t.db}::${t.table}`);
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: a.toLowerCase(),
        keyword: 'table',
        if_not_exists: ife,
        table: [t],
        create_definitions: cd
      }
    };
  }

if_not_exists_stmt
  = 'IF'i __ 'NOT'i __ 'EXISTS'i { return 'if not exists'; }

create_column_definition
  = head:create_definition tail:(__ COMMA __ create_definition)* {
    return createList(head, tail, 3);
  }

create_definition
  = column_definition

column_definition
  = c:column_ref __ d:data_type __ ca:column_constraint_list? {
    columnList.add(`create::${c.table}::${c.column}`);
    const constraint = ca || {};
    return {
      column: c,
      definition: d,
      resource: 'column',
      nullable: constraint.nullable,
      default_val: constraint.default_val,
      comment: constraint.comment
    };
  }

column_constraint_list
  = head:column_constraint tail:(__ column_constraint)* {
    const result = [head];
    for (let i = 0; i < tail.length; i++) {
      result.push(tail[i][1]);
    }
    const constraints = {};
    for (let constraint of result) {
      if (constraint.type === 'not null') constraints.nullable = { type: 'not null', value: 'not null' };
      if (constraint.type === 'default') constraints.default_val = constraint;
      if (constraint.type === 'comment') constraints.comment = constraint;
    }
    return constraints;
  }

column_constraint
  = KW_NOT __ KW_NULL {
    return {
      type: 'not null',
      value: 'not null'
    };
  }
  / KW_DEFAULT __ ce:expr {
    return {
      type: 'default',
      value: ce
    };
  }
  / comment

comment
  = k:KW_COMMENT __ s:KW_ASSIGIN_EQUAL? __ c:literal_string {
    return {
      type: 'comment',
      keyword: k.toLowerCase(),
      symbol: s,
      value: c
    };
  }

drop_stmt
  = a:KW_DROP __ r:KW_TABLE __ ife:if_exists? __ t:table_ref_list {
    if(t) t.forEach(tt => tableList.add(`${a}::${tt.db}::${tt.table}`));
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: a.toLowerCase(),
        keyword: r.toLowerCase(),
        prefix: ife,
        name: t
      }
    };
  }
  / a:KW_DROP __ r:KW_DATABASE __ ife:if_exists? __ t:ident_name {
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: a.toLowerCase(),
        keyword: r.toLowerCase(),
        prefix: ife,
        name: t
      }
    };
  }

if_exists
  = 'IF'i __ 'EXISTS'i { return 'if exists'; }

set_stmt
  = KW_SET __ assign_stmt {
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'set',
        expr: assign_stmt
      }
    };
  }

assign_stmt
  = head:assign_item tail:(__ COMMA __ assign_item)* {
    return createList(head, tail, 3);
  }

assign_item
  = tg:var_decl __ s:KW_ASSIGIN_EQUAL __ v:literal {
    return {
      left: tg,
      symbol: s,
      right: v
    };
  }

union_stmt
  = head:select_stmt tail:(__ KW_UNION __ KW_ALL? __ select_stmt)* {
    const unions = [];
    for (let i = 0; i < tail.length; i++) {
      unions.push({
        type: 'union',
        set_type: tail[i][3] ? 'union_all' : 'union',
        union: tail[i][5] || tail[i][4]
      });
    }
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        with: null,
        type: 'select',
        options: head.options,
        distinct: head.distinct,
        columns: head.columns,
        from: head.from,
        where: head.where,
        groupby: head.groupby,
        having: head.having,
        orderby: head.orderby,
        limit: head.limit,
        for_update: null,
        _next: unions
      }
    };
  }

select_stmt
  = KW_SELECT __
    opts:option_clause? __
    d:KW_DISTINCT? __
    c:column_clause __
    f:from_clause? __
    w:where_clause? __
    g:group_by_clause? __
    h:having_clause? __
    o:order_by_clause? __
    l:limit_clause? {
    if(f) f.forEach(tbl => {
      const { db, table, as } = tbl
      tableList.add(`select::${db}::${table}`)
    })
    return {
      type: 'select',
      options: opts,
      distinct: d,
      columns: c,
      from: f,
      where: w,
      groupby: g,
      having: h,
      orderby: o,
      limit: l
    };
  }

option_clause
  = head:query_option tail:(__ query_option)* {
    const opts = [head];
    for (let i = 0; i < tail.length; i++) {
      opts.push(tail[i][1]);
    }
    return opts;
  }

query_option
  = option:(
    'HIGH_PRIORITY'i
    / 'STRAIGHT_JOIN'i
    / 'SQL_SMALL_RESULT'i
    / 'SQL_BIG_RESULT'i
    / 'SQL_BUFFER_RESULT'i
    / 'SQL_CACHE'i
    / 'SQL_NO_CACHE'i
    / 'SQL_CALC_FOUND_ROWS'i
  ) {
    return option.toUpperCase();
  }

column_clause
  = head:column_list_item tail:(__ COMMA __ column_list_item)* {
    return createList(head, tail, 3);
  }

column_list_item
  = t:table_name __ DOT __ STAR {
    return {
      expr: {
        type: 'column_ref',
        table: t.table,
        column: '*'
      },
      as: null
    };
  }
  / STAR {
    return {
      expr: {
        type: 'column_ref',
        table: null,
        column: '*'
      },
      as: null
    };
  }
  / e:expr __ alias:alias_clause? {
    return {
      expr: e,
      as: alias
    };
  }

from_clause
  = KW_FROM __ t:table_ref_list { return t; }

table_ref_list
  = head:table_factor tail:(__ COMMA __ table_factor)* {
    return createList(head, tail, 3);
  }

table_factor
  = t:table_name __ alias:alias_clause? {
    if (t) {
      tableList.add(`select::${t.db}::${t.table}`);
    }
    return {
      db: t && t.db,
      table: t && t.table,
      as: alias
    };
  }
  / LPAREN __ stmt:union_stmt __ RPAREN __ alias:alias_clause? {
    stmt.parentheses = true;
    return {
      expr: stmt,
      as: alias
    };
  }

table_name
  = dt:ident tail:(__ DOT __ ident)? {
    const obj = { db: null, table: dt };
    if (tail !== null) {
      obj.db = dt;
      obj.table = tail[3];
    }
    return obj;
  }

alias_clause
  = KW_AS __ i:alias_ident { return i; }
  / alias_ident

alias_ident
  = i:ident_name !{ return reservedMap[i.toUpperCase()] === true; } { return i; }
  / i:quoted_ident { return i; }

where_clause
  = KW_WHERE __ e:expr { return e; }

group_by_clause
  = KW_GROUP __ KW_BY __ l:column_ref_list { return l; }

column_ref_list
  = head:column_ref tail:(__ COMMA __ column_ref)* {
    return createList(head, tail, 3);
  }

having_clause
  = KW_HAVING __ e:expr { return e; }

order_by_clause
  = KW_ORDER __ KW_BY __ l:order_by_list { return l; }

order_by_list
  = head:order_by_element tail:(__ COMMA __ order_by_element)* {
    return createList(head, tail, 3);
  }

order_by_element
  = e:expr __ d:order_by_direction? {
    const obj = { expr: e, type: 'ASC' };
    if (d === 'DESC') obj.type = 'DESC';
    return obj;
  }

order_by_direction
  = KW_DESC { return 'DESC'; }
  / KW_ASC { return 'ASC'; }

limit_clause
  = KW_LIMIT __ i1:literal_numeric __ tail:(COMMA __ literal_numeric)? {
    const obj = [{
      type: 'number',
      value: i1
    }];
    if (tail === null) return obj;
    return [{
      type: 'number',
      value: tail[2]
    }, {
      type: 'number',
      value: i1
    }];
  }

insert_stmt
  = KW_INSERT __ 'INTO'i? __ t:table_name __ cols:(LPAREN __ l:column_list __ RPAREN __)? v:value_clause {
    if (t) tableList.add(`insert::${t.db}::${t.table}`);
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'insert',
        table: [t],
        columns: cols ? cols[2] : null,
        values: v
      }
    };
  }

replace_insert_stmt
  = r:KW_REPLACE __ 'INTO'i? __ t:table_name __ cols:(LPAREN __ l:column_list __ RPAREN __)? v:value_clause {
    if (t) tableList.add(`insert::${t.db}::${t.table}`);
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: r.toLowerCase(),
        table: [t],
        columns: cols ? cols[2] : null,
        values: v
      }
    };
  }

column_list
  = head:column_name tail:(__ COMMA __ column_name)* {
    columnList.add(`insert::${head}::${head}`);
    const cols = [{ type: 'double_quote_string', value: head }];
    for (let i = 0; i < tail.length; i++) {
      columnList.add(`insert::${tail[i][3]}::${tail[i][3]}`);
      cols.push({ type: 'double_quote_string', value: tail[i][3] });
    }
    return cols;
  }

value_clause
  = KW_VALUES __ l:value_list { return l; }

value_list
  = head:value_item tail:(__ COMMA __ value_item)* {
    return createList(head, tail, 3);
  }

value_item
  = LPAREN __ l:expr_list __ RPAREN {
    return {
      type: 'expr_list',
      value: l,
      separator: ', '
    };
  }

expr_list
  = head:expr tail:(__ COMMA __ expr)* {
    return createExprList(head, tail);
  }

update_stmt
  = KW_UPDATE __ t:table_ref_list __ KW_SET __ l:update_set_list __ w:where_clause? {
    if (t) t.forEach(table => tableList.add(`update::${table.db}::${table.table}`));
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'update',
        table: t,
        set: l,
        where: w
      }
    };
  }

update_set_list
  = head:update_set_item tail:(__ COMMA __ update_set_item)* {
    return createList(head, tail, 3);
  }

update_set_item
  = c:column_ref __ s:KW_ASSIGIN_EQUAL __ v:expr {
    return {
      column: c.column,
      value: v,
      table: c.table
    };
  }

delete_stmt
  = KW_DELETE __ f:KW_FROM __ t:table_name __ w:where_clause? {
    if (t) tableList.add(`delete::${t.db}::${t.table}`);
    return {
      tableList: Array.from(tableList),
      columnList: columnListTableAlias(columnList),
      ast: {
        type: 'delete',
        from: [t],
        table: [t],
        where: w
      }
    };
  }

expr
  = or_expr

or_expr
  = head:and_expr tail:(__ KW_OR __ and_expr)* {
    return tail.reduce((result, element) => {
      return createBinaryExpr(element[1], result, element[3]);
    }, head);
  }

and_expr
  = head:not_expr tail:(__ KW_AND __ not_expr)* {
    return tail.reduce((result, element) => {
      return createBinaryExpr(element[1], result, element[3]);
    }, head);
  }

not_expr
  = KW_NOT __ e:not_expr {
    return createUnaryExpr('NOT', e);
  }
  / comparison_expr

comparison_expr
  = left:additive_expr __ rh:comparison_op_right? {
    if (rh === null) return left;
    else if (rh.type === 'arithmetic') return createBinaryExpr(rh.op, left, rh.right);
    else return rh.op(left, rh.right);
  }

comparison_op_right
  = KW_BETWEEN __ begin:additive_expr __ KW_AND __ end:additive_expr {
    return {
      op: function(left, right) {
        return {
          type: 'binary_expr',
          operator: 'BETWEEN',
          left: left,
          right: {
            type: 'expr_list',
            value: [begin, end]
          }
        };
      },
      right: null
    };
  }
  / op:comparison_operator __ right:additive_expr {
    return { type: 'arithmetic', op: op, right: right };
  }
  / KW_IS __ KW_NOT? __ KW_NULL {
    return {
      op: function(left, right) {
        return createBinaryExpr('IS', left, createUnaryExpr('NOT', { type: 'null', value: null }));
      },
      right: null
    };
  }
  / KW_IN __ LPAREN __ l:expr_list __ RPAREN {
    return {
      op: function(left, right) {
        return createBinaryExpr('IN', left, { type: 'expr_list', value: l, parentheses: true });
      },
      right: null
    };
  }
  / KW_LIKE __ right:additive_expr {
    return { type: 'arithmetic', op: 'LIKE', right: right };
  }

comparison_operator
  = ">=" { return '>='; }
  / ">" { return '>'; }
  / "<=" { return '<='; }
  / "<>" { return '<>'; }
  / "!=" { return '!='; }
  / "<" { return '<'; }
  / "=" { return '='; }

additive_expr
  = head:multiplicative_expr tail:(__ additive_operator __ multiplicative_expr)* {
    return tail.reduce((result, element) => {
      return createBinaryExpr(element[1], result, element[3]);
    }, head);
  }

additive_operator
  = "+" { return '+'; }
  / "-" { return '-'; }

multiplicative_expr
  = head:primary_expr tail:(__ multiplicative_operator __ primary_expr)* {
    return tail.reduce((result, element) => {
      return createBinaryExpr(element[1], result, element[3]);
    }, head);
  }

multiplicative_operator
  = "*" { return '*'; }
  / "/" { return '/'; }
  / "%" { return '%'; }

primary_expr
  = array_subscript_expr
  / literal
  / func_call
  / column_ref
  / param
  / LPAREN __ e:expr __ RPAREN {
    e.parentheses = true;
    return e;
  }

array_subscript_expr
  = base:(func_call / column_ref) __ indices:array_index+ {
    return {
      type: 'array_access',
      expr: base,
      indices: indices
    };
  }

array_index
  = LBRACK __ index:expr __ RBRACK {
    return index;
  }

func_call
  = name:ident_name __ LPAREN __ l:expr_list? __ RPAREN {
    return {
      type: 'function',
      name: name,
      args: l ? { type: 'expr_list', value: l } : null
    };
  }

column_ref
  = tbl:ident __ DOT __ col:column_name {
    columnList.add(`select::${col}::${tbl}`);
    return {
      type: 'column_ref',
      table: tbl,
      column: col
    };
  }
  / col:column_name {
    columnList.add(`select::${col}::null`);
    return {
      type: 'column_ref',
      table: null,
      column: col
    };
  }

column_name
  = name:ident_name !{ return reservedMap[name.toUpperCase()] === true; } { return name; }
  / name:quoted_ident { return name; }

param
  = l:literal_string {
    return {
      type: 'param',
      value: l.value
    };
  }

literal
  = literal_object
  / literal_string
  / literal_numeric
  / literal_bool
  / literal_null
  / literal_datetime

literal_string
  = "'" chars:single_char* "'" {
    return {
      type: 'single_quote_string',
      value: chars.join('')
    };
  }
  / '"' chars:double_char* '"' {
    return {
      type: 'double_quote_string',
      value: chars.join('')
    };
  }

single_char
  = !"'" c:. { return c; }

double_char
  = !'"' c:. { return c; }

literal_numeric
  = n:number {
    return { type: 'number', value: n };
  }

number
  = int_:int frac:frac? exp:exp? { return parseFloat(int_ + frac + exp); }
  / int_:int frac:frac? { return parseFloat(int_ + frac); }
  / int_:int exp:exp? { return parseFloat(int_ + exp); }
  / int_:int { return parseInt(int_); }

int
  = digits
  / digit19:digit19 digits:digits { return digit19 + digits; }
  / "-" digits:digits { return "-" + digits; }
  / "-" digit19:digit19 digits:digits { return "-" + digit19 + digits; }

frac
  = "." digits:digits { return "." + digits; }

exp
  = e:e digits:digits { return e + digits; }

digits
  = digits:digit+ { return digits.join(''); }

digit
  = [0-9]

digit19
  = [1-9]

e
  = [eE] [+-]?

literal_bool
  = KW_TRUE { return { type: 'bool', value: true }; }
  / KW_FALSE { return { type: 'bool', value: false }; }

literal_null
  = KW_NULL { return { type: 'null', value: null }; }

literal_datetime
  = type:(KW_DATE / KW_TIME / KW_TIMESTAMP) __ s:literal_string {
    return {
      type: type.toLowerCase(),
      value: s.value
    };
  }

literal_object
  = "{" __ pairs:object_pair_list? __ "}" {
    return {
      type: 'object',
      value: pairs || []
    };
  }

object_pair_list
  = head:object_pair tail:(__ "," __ object_pair)* {
    return [head].concat(tail.map(t => t[3]));
  }

object_pair
  = key:literal_string __ ":" __ value:literal {
    return { key: key.value, value: value };
  }

var_decl
  = "$" name:ident_name {
    return {
      type: 'var',
      name: name
    };
  }

quoted_ident
  = '"' chars:( !'"' . )+ '"' { return chars.map(c => c[1]).join(''); }
  / '`' chars:( !'`' . )+ '`' { return chars.map(c => c[1]).join(''); }

ident_name
  = start:ident_start parts:ident_part* { return start + parts.join(''); }

ident_part
  = [A-Za-z0-9_]

ident
  = name:ident_name !{ return reservedMap[name.toUpperCase()] === true; } { return name; }
  / name:quoted_ident { return name; }

// DuckDB-specific data types
data_type
  = duckdb_data_type
  / character_string_type
  / numeric_type
  / datetime_type
  / boolean_type

duckdb_data_type
  = t:KW_UUID { return { dataType: t }; }
  / t:KW_JSON { return { dataType: t }; }
  / t:(KW_HUGEINT / KW_BIGINT / KW_INTEGER / KW_SMALLINT / KW_TINYINT) { return { dataType: t }; }
  / t:(KW_DOUBLE / KW_FLOAT) { return { dataType: t }; }
  / t:(KW_BLOB / KW_BYTEA / KW_BINARY / KW_VARBINARY) { return { dataType: t }; }
  / t:KW_ARRAY __ LPAREN __ dt:data_type __ RPAREN { return { dataType: t, elementType: dt }; }
  / t:KW_LIST __ LPAREN __ dt:data_type __ RPAREN { return { dataType: t, elementType: dt }; }
  / t:KW_STRUCT __ LPAREN __ fields:struct_field_list __ RPAREN { return { dataType: t, fields: fields }; }
  / t:KW_MAP __ LPAREN __ kt:data_type __ COMMA __ vt:data_type __ RPAREN { 
    return { dataType: t, keyType: kt, valueType: vt }; 
  }
  / t:KW_UNION __ LPAREN __ types:data_type_list __ RPAREN { return { dataType: t, memberTypes: types }; }

struct_field_list
  = head:struct_field tail:(__ COMMA __ struct_field)* {
    return createList(head, tail, 3);
  }

struct_field
  = name:ident __ dt:data_type {
    return { name: name, type: dt };
  }

data_type_list
  = head:data_type tail:(__ COMMA __ data_type)* {
    return createList(head, tail, 3);
  }

character_string_type
  = t:(KW_CHAR / KW_VARCHAR) __ LPAREN __ length:literal_numeric __ RPAREN {
    return { dataType: t, length: length.value };
  }
  / t:(KW_CHAR / KW_VARCHAR) { return { dataType: t }; }

numeric_type
  = t:(KW_INT / KW_INTEGER) { return { dataType: t }; }
  / t:KW_DECIMAL __ LPAREN __ precision:literal_numeric __ COMMA __ scale:literal_numeric __ RPAREN {
    return { dataType: t, precision: precision.value, scale: scale.value };
  }
  / t:KW_DECIMAL { return { dataType: t }; }
  / t:(KW_FLOAT / KW_DOUBLE) { return { dataType: t }; }

datetime_type
  = t:(KW_DATE / KW_TIME / KW_TIMESTAMP) { return { dataType: t }; }
  / t:KW_INTERVAL { return { dataType: t }; }

boolean_type
  = t:(KW_BOOLEAN / KW_BOOL / KW_LOGICAL) { return { dataType: t }; }

// Keywords
KW_ADD = "ADD"i !ident_start { return 'ADD'; }
KW_ALL = "ALL"i !ident_start { return 'ALL'; }
KW_ALTER = "ALTER"i !ident_start { return 'ALTER'; }
KW_AND = "AND"i !ident_start { return 'AND'; }
KW_ARRAY = "ARRAY"i !ident_start { return 'ARRAY'; }
KW_AS = "AS"i !ident_start { return 'AS'; }
KW_ASC = "ASC"i !ident_start { return 'ASC'; }
KW_ASSIGIN_EQUAL = "=" { return '='; }
KW_BETWEEN = "BETWEEN"i !ident_start { return 'BETWEEN'; }
KW_BIGINT = "BIGINT"i !ident_start { return 'BIGINT'; }
KW_BINARY = "BINARY"i !ident_start { return 'BINARY'; }
KW_BLOB = "BLOB"i !ident_start { return 'BLOB'; }
KW_BOOL = "BOOL"i !ident_start { return 'BOOL'; }
KW_BOOLEAN = "BOOLEAN"i !ident_start { return 'BOOLEAN'; }
KW_BY = "BY"i !ident_start { return 'BY'; }
KW_BYTEA = "BYTEA"i !ident_start { return 'BYTEA'; }
KW_CASE = "CASE"i !ident_start { return 'CASE'; }
KW_CHAR = "CHAR"i !ident_start { return 'CHAR'; }
KW_CHECK = "CHECK"i !ident_start { return 'CHECK'; }
KW_COLLATE = "COLLATE"i !ident_start { return 'COLLATE'; }
KW_COLUMN = "COLUMN"i !ident_start { return 'COLUMN'; }
KW_COMMENT = "COMMENT"i !ident_start { return 'COMMENT'; }
KW_CREATE = "CREATE"i !ident_start { return 'CREATE'; }
KW_CROSS = "CROSS"i !ident_start { return 'CROSS'; }
KW_CURRENT_DATE = "CURRENT_DATE"i !ident_start { return 'CURRENT_DATE'; }
KW_CURRENT_TIME = "CURRENT_TIME"i !ident_start { return 'CURRENT_TIME'; }
KW_CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"i !ident_start { return 'CURRENT_TIMESTAMP'; }
KW_DATABASE = "DATABASE"i !ident_start { return 'DATABASE'; }
KW_DATABASES = "DATABASES"i !ident_start { return 'DATABASES'; }
KW_DATE = "DATE"i !ident_start { return 'DATE'; }
KW_DATETIME = "DATETIME"i !ident_start { return 'DATETIME'; }
KW_DECIMAL = "DECIMAL"i !ident_start { return 'DECIMAL'; }
KW_DEFAULT = "DEFAULT"i !ident_start { return 'DEFAULT'; }
KW_DELETE = "DELETE"i !ident_start { return 'DELETE'; }
KW_DESC = "DESC"i !ident_start { return 'DESC'; }
KW_DESCRIBE = "DESCRIBE"i !ident_start { return 'DESCRIBE'; }
KW_DISTINCT = "DISTINCT"i !ident_start { return 'DISTINCT'; }
KW_DOUBLE = "DOUBLE"i !ident_start { return 'DOUBLE'; }
KW_DROP = "DROP"i !ident_start { return 'DROP'; }
KW_ELSE = "ELSE"i !ident_start { return 'ELSE'; }
KW_END = "END"i !ident_start { return 'END'; }
KW_ENUM = "ENUM"i !ident_start { return 'ENUM'; }
KW_EXISTS = "EXISTS"i !ident_start { return 'EXISTS'; }
KW_EXPLAIN = "EXPLAIN"i !ident_start { return 'EXPLAIN'; }
KW_FALSE = "FALSE"i !ident_start { return 'FALSE'; }
KW_FLOAT = "FLOAT"i !ident_start { return 'FLOAT'; }
KW_FROM = "FROM"i !ident_start { return 'FROM'; }
KW_FULL = "FULL"i !ident_start { return 'FULL'; }
KW_FUNCTION = "FUNCTION"i !ident_start { return 'FUNCTION'; }
KW_GROUP = "GROUP"i !ident_start { return 'GROUP'; }
KW_HAVING = "HAVING"i !ident_start { return 'HAVING'; }
KW_HUGEINT = "HUGEINT"i !ident_start { return 'HUGEINT'; }
KW_IF = "IF"i !ident_start { return 'IF'; }
KW_IN = "IN"i !ident_start { return 'IN'; }
KW_INDEX = "INDEX"i !ident_start { return 'INDEX'; }
KW_INNER = "INNER"i !ident_start { return 'INNER'; }
KW_INSERT = "INSERT"i !ident_start { return 'INSERT'; }
KW_INT = "INT"i !ident_start { return 'INT'; }
KW_INTEGER = "INTEGER"i !ident_start { return 'INTEGER'; }
KW_INTERVAL = "INTERVAL"i !ident_start { return 'INTERVAL'; }
KW_INTO = "INTO"i !ident_start { return 'INTO'; }
KW_IS = "IS"i !ident_start { return 'IS'; }
KW_JOIN = "JOIN"i !ident_start { return 'JOIN'; }
KW_JSON = "JSON"i !ident_start { return 'JSON'; }
KW_KEY = "KEY"i !ident_start { return 'KEY'; }
KW_LEFT = "LEFT"i !ident_start { return 'LEFT'; }
KW_LIKE = "LIKE"i !ident_start { return 'LIKE'; }
KW_LIMIT = "LIMIT"i !ident_start { return 'LIMIT'; }
KW_LIST = "LIST"i !ident_start { return 'LIST'; }
KW_LOGICAL = "LOGICAL"i !ident_start { return 'LOGICAL'; }
KW_MAP = "MAP"i !ident_start { return 'MAP'; }
KW_NOT = "NOT"i !ident_start { return 'NOT'; }
KW_NULL = "NULL"i !ident_start { return 'NULL'; }
KW_ON = "ON"i !ident_start { return 'ON'; }
KW_OR = "OR"i !ident_start { return 'OR'; }
KW_ORDER = "ORDER"i !ident_start { return 'ORDER'; }
KW_OUTER = "OUTER"i !ident_start { return 'OUTER'; }
KW_PRIMARY = "PRIMARY"i !ident_start { return 'PRIMARY'; }
KW_REPLACE = "REPLACE"i !ident_start { return 'REPLACE'; }
KW_RIGHT = "RIGHT"i !ident_start { return 'RIGHT'; }
KW_SELECT = "SELECT"i !ident_start { return 'SELECT'; }
KW_SET = "SET"i !ident_start { return 'SET'; }
KW_SHOW = "SHOW"i !ident_start { return 'SHOW'; }
KW_SMALLINT = "SMALLINT"i !ident_start { return 'SMALLINT'; }
KW_STRING = "STRING"i !ident_start { return 'STRING'; }
KW_STRUCT = "STRUCT"i !ident_start { return 'STRUCT'; }
KW_TABLE = "TABLE"i !ident_start { return 'TABLE'; }
KW_TABLES = "TABLES"i !ident_start { return 'TABLES'; }
KW_THEN = "THEN"i !ident_start { return 'THEN'; }
KW_TIME = "TIME"i !ident_start { return 'TIME'; }
KW_TIMESTAMP = "TIMESTAMP"i !ident_start { return 'TIMESTAMP'; }
KW_TINYINT = "TINYINT"i !ident_start { return 'TINYINT'; }
KW_TRUE = "TRUE"i !ident_start { return 'TRUE'; }
KW_TYPE = "TYPE"i !ident_start { return 'TYPE'; }
KW_UNION = "UNION"i !ident_start { return 'UNION'; }
KW_UPDATE = "UPDATE"i !ident_start { return 'UPDATE'; }
KW_USE = "USE"i !ident_start { return 'USE'; }
KW_USING = "USING"i !ident_start { return 'USING'; }
KW_UUID = "UUID"i !ident_start { return 'UUID'; }
KW_VALUES = "VALUES"i !ident_start { return 'VALUES'; }
KW_VARBINARY = "VARBINARY"i !ident_start { return 'VARBINARY'; }
KW_VARCHAR = "VARCHAR"i !ident_start { return 'VARCHAR'; }
KW_WHEN = "WHEN"i !ident_start { return 'WHEN'; }
KW_WHERE = "WHERE"i !ident_start { return 'WHERE'; }
KW_WITH = "WITH"i !ident_start { return 'WITH'; }

// Operators
DOT = "."
STAR = "*"
LPAREN = "("
RPAREN = ")"
LBRACK = "["
RBRACK = "]"
COMMA = ","
SEMICOLON = ";"

// Whitespace
__ = whitespace*

whitespace
  = [ \t\n\r]
  / comment_line
  / comment_block

comment_line
  = "//" [^\n\r]*
  / "--" [^\n\r]*

comment_block
  = "/*" (!"*/" .)* "*/"

keyword = (
  KW_ADD / KW_ALL / KW_ALTER / KW_AND / KW_ARRAY / KW_AS / KW_ASC /
  KW_BETWEEN / KW_BIGINT / KW_BINARY / KW_BLOB / KW_BOOL / KW_BOOLEAN / KW_BY / KW_BYTEA /
  KW_CASE / KW_CHAR / KW_CHECK / KW_COLLATE / KW_COLUMN / KW_COMMENT / KW_CREATE / KW_CROSS /
  KW_CURRENT_DATE / KW_CURRENT_TIME / KW_CURRENT_TIMESTAMP /
  KW_DATABASE / KW_DATABASES / KW_DATE / KW_DATETIME / KW_DECIMAL / KW_DEFAULT / KW_DELETE / 
  KW_DESC / KW_DESCRIBE / KW_DISTINCT / KW_DOUBLE / KW_DROP / KW_ELSE / KW_END / KW_ENUM /
  KW_EXISTS / KW_EXPLAIN / KW_FALSE / KW_FLOAT / KW_FROM / KW_FULL / KW_FUNCTION / KW_GROUP / 
  KW_HAVING / KW_HUGEINT / KW_IF / KW_IN / KW_INDEX / KW_INNER / KW_INSERT / KW_INT / KW_INTEGER / 
  KW_INTERVAL / KW_INTO / KW_IS / KW_JOIN / KW_JSON / KW_KEY / KW_LEFT / KW_LIKE / KW_LIMIT / 
  KW_LIST / KW_LOGICAL / KW_MAP / KW_NOT / KW_NULL / KW_ON / KW_OR / KW_ORDER / KW_OUTER / 
  KW_PRIMARY / KW_REPLACE / KW_RIGHT / KW_SELECT / KW_SET / KW_SHOW / KW_SMALLINT / KW_STRING / 
  KW_STRUCT / KW_TABLE / KW_TABLES / KW_THEN / KW_TIME / KW_TIMESTAMP / KW_TINYINT / KW_TRUE / 
  KW_TYPE / KW_UNION / KW_UPDATE / KW_USE / KW_USING / KW_UUID / KW_VALUES / KW_VARBINARY / 
  KW_VARCHAR / KW_WHEN / KW_WHERE / KW_WITH
)

ident_start = [A-Za-z_]
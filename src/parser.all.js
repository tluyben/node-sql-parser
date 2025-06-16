import { parse as athena } from '../pegjs/athena.pegjs'
import { parse as bigquery } from '../pegjs/bigquery.pegjs'
import { parse as clickhouse } from '../pegjs/clickhouse.pegjs'
import { parse as db2 } from '../pegjs/db2.pegjs'
import { parse as flinksql } from '../pegjs/flinksql.pegjs'
import { parse as hive } from '../pegjs/hive.pegjs'
import { parse as mysql } from '../pegjs/mysql.pegjs'
import { parse as mariadb } from '../pegjs/mariadb.pegjs'
import { parse as noql } from '../pegjs/noql.pegjs'
import { parse as postgresql } from '../pegjs/postgresql.pegjs'
import { parse as redshift } from '../pegjs/redshift.pegjs'
import { parse as sqlite } from '../pegjs/sqlite.pegjs'
import { parse as transactsql } from '../pegjs/transactsql.pegjs'
import { parse as snowflake } from '../pegjs/snowflake.pegjs'
import { parse as trino } from '../pegjs/trino.pegjs'

export default {
  athena,
  bigquery,
  clickhouse,
  db2,
  flinksql,
  hive,
  mysql,
  mariadb,
  noql,
  postgresql,
  redshift,
  snowflake,
  sqlite,
  transactsql,
  trino,
}

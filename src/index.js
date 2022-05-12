const {
  queryInsert,
  querySelect,
  querySelectGroup,
  querySelectJoin,
  queryUpdate,
  querySelectJoinGroup
} = require('ab-dbquery')
const mariadb = require('mariadb')


/**
 * Returns number of rows in table that match where condition.
 *
 * @param {String} table Table name to be used in `SELECT` query statement.
 * @param {String|Object} [where=null] Where condition to be used in `SELECT` query statement.
 * @throws 'Not passed table name to be used in query statement!'
 * @returns {Number} Number of rows in table that match where condition.
 */
const count = async (table, where) => {
  const rows = await query(querySelect(table, `COUNT(*) AS count`))
  return rows[0].count || 0

}

/**
 * Returns MariaDB pool that created using passed configuration information.
 *
 * @param {Object} config Configuration information for using MariaDB pool.
 * @param {String} [config.host='127.0.0.1'] IP address or DNS of MariaDB server.
 * @param {Number} [config.port=3306] MariaDB server port number.
 * @param {String} config.database Default database to use when establishing connection.
 * @param {String} config.user User to access MariaDB.
 * @param {String} config.password User password.
 * @param {Number} [config.connectionLimit=10] Maximum number of connection in pool.
 * @param {Boolean} [config.compress=false] Compresses the exchange with the database through gzip.
 * This permits better performance when the database is not in the same location.
 * @param {winston.Logger|console} [config.logger=console] Logger for log output.
 * @throws 'Configuration information required to create MariaDB pool was not passed!'
 * @throws 'MariaDB pool not created!'
 * @returns {mariadb.Pool} MariaDB pool created using passed configuration information.
 */
const createPool = config => {
  if (mariadb.pool) {
    poolInfo()
    return mariadb.pool
  }

  if (!config) {
    if (!mariadb.config) {
      throw `[MariaDB createPool] Configuration information required to create MariaDB pool was not passed!`
    } else {
      config = mariadb.config
    }
  }

  mariadb.config = config

  mariadb.log = config?.logger || console
  mariadb.pool = mariadb.createPool(config)

  if (!mariadb.pool) {
    throw `[MariaDB createPool] MariaDB pool not created!`
  }

  return mariadb.pool
}

/**
 * Returns MariaDB connection that created using passed configuration imformation.
 *
 * @async
 * @param {Object} config Configuration information for using MariaDB connection.
 * @param {String} [config.host='127.0.0.1'] IP address or DNS of MariaDB server.
 * @param {Number} [config.port=3306] MariaDB server port number.
 * @param {String} config.database Default database to use when establishing connection.
 * @param {String} config.user User to access MariaDB.
 * @param {String} config.password User password.
 * @param {Boolean} [config.compress=false] Compresses the exchange with the database through gzip.
 * This permits better performance when the database is not in the same location.
 * @param {winston.Logger|console} [config.logger=console] Logger for log output.
 * @throws 'Configuration information required to create MariaDB connection was not passed!'
 * @throws 'MariaDB connection not created!'
 * @returns {Promise<mariadb.Connection>} MariaDB connection created using passed configuration information.
 */
const getConnection = async config => {
  if (mariadb.pool && !mariadb.connection) {
    mariadb.connection = await mariadb.pool.getConnection()
    poolInfo()
    return mariadb.connection
  }

  if (mariadb?.connection?.isValid()) {
    return mariadb.connection
  }

  if (!config) {
    if (!mariadb.config) {
      throw `[MariaDB getConnection] Configuration information required to create MariaDB connection was not passed!`
    } else {
      config = mariadb.config
    }
  }

  mariadb.config = config

  mariadb.log = config?.logger || console
  mariadb.connection = await mariadb.createConnection(config)

  if (!mariadb.connection) {
    throw `[MariaDB getConnection] MariaDB connection not created!`
  }

  return mariadb.connection
}

/**
 * Returns result after executing `INSERT` query statement.
 *
 * @param {String} table Table name to use in query statement.
 * @param {Object} values Values object that consisting of field names and values to add to table.
 * @throws 'Not passed table name to be used in query statement!'
 * @throws 'Not passed object consisting of field and value to be used in INSERT query statement!'
 * @returns {Promise<Any>} Result of executing `INSERT` query statement.
 */
const insert = async (table, values) => {
  return await query(queryInsert(table, values))
}

/**
 * MariaDB pool connection count information output.
 */
const poolInfo = () => {
  if (!mariadb?.pool) return

  mariadb.log?.info && mariadb.log.info(
    `[MariaDB pool] connections - ` +
    `active: ${mariadb.pool?.activeConnections()} / ` +
    `idle: ${mariadb.pool?.idleConnections()} / ` +
    `total: ${mariadb.pool?.totalConnections()}`
  )
}

/**
 * Run query statement and returns result.
 *
 * @param {String} query Query statement to be run.
 * @throws '[MariaDB query] No connection to MariaDB!'
 * @returns {Promise<Any>} Result of run query statement.
 */
const query = async query => {
  const connection = await getConnection()

  if (!connection || !connection.isValid()) {
    throw `[MariaDB query] No connection to MariaDB!`
  }

  try {
    mariadb.log?.debug && mariadb.log.debug(`[MariaDB query] ${query}`)
    return await connection.query(query)
  } catch (error) {
    mariadb.log?.error && mariadb.log.error(`[MariaDB query] error: %o`, error)
    throw error
  } finally {
    connection.release && connection.release()
    connection.end && connection.end()
  }
}

/**
 * Returns result after executing `SELECT` query statement.
 *
 * @param {String} table Table name to be used in `SELECT` query statement.
 * @param {String|Array} [field=null] Fields to be used in `SELECT` query statement.
 * @param {String|Object} [where=null] Where condition to be used in `SELECT` query statement.
 * @param {String} [order=null] Order by clause to be used in `SELECT` query statement.
 * @param {Number} [limit=0] Number of rows to return to be used in `SELECT` query statement.
 * If `0` no limit in used.
 * @throws 'Not passed table name to be used in query statement!'
 * @returns {Promise<Any>} Result of executing `SELECT` query statement.
 */
const select = async (table, field = null, where = null, order = null, limit = 0) => {
  return await query(querySelect(table, field, where, order, limit))
}

/**
 * Returns result after executing `SELECT` query statement using group.
 *
 * @param {String} table Table name to use in query statement.
 * @param {String|Array} [field=null] Fields to be used in query statement.
 * @param {String|Object} [where=null] Where condition to be used in query statement.
 * @param {String} [group=null] Group by clause to be used in query statement.
 * @param {String} [having=null] Having condition to be used in group by clause of query statement.
 * @param {String} [order=null] Order by clause to be used in query statement.
 * @param {Number} [limit=0] Number of rows to return to be used in query statement. If `0` no limit in used.
 * @throws 'Not passed table name to be used in query statement!'
 * @returns {Promise<Any>} Result of executing `SELECT` query statement using group.
 */
const selectGroup = async (
  table,
  field = null,
  where = null,
  group = null,
  having = null,
  order = null,
  limit = 0
) => {
  return await query(querySelectGroup(table, field, where, group, having, order, limit))
}

/**
 * Returns result after executing `SELECT` query statement using table join.
 *
 * @param {String} table Table name to use in query statement.
 * @param {String} [type=null] Join type to be used in table join query statement.
 * @param {String} [join=null] Table name of joined target table to use in join query statement.
 * @param {String} [on=null] Constraint for to use table join.
 * @param {String|Array} [field=null] Fields to be used in query statement.
 * @param {String|Object} [where=null] Where condition to be used in query statement.
 * @param {String} [order=null] Order by clause to be used in query statement.
 * @param {Number} [limit=0] Number of rows to return to be used in query statement. If `0` no limit in used.
 * @throws 'Not passed table name to be used in query statement!'
 * @returns {Promise<Any>} Result of executing `SELECT` query statement using table join.
 */
const selectJoin = async (
  table,
  type = null,
  join = null,
  on = null,
  field = null,
  where = null,
  order = null,
  limit = 0
) => {
  return await query(querySelectJoin(table, type, join, on, field, where, order, limit))
}

/**
 * Returns result after executing `SELECT` query statement using table join and group.
 *
 * @param {String} table Table name to use in query statement.
 * @param {String} [type=null] Join type to be used in table join query statement.
 * @param {String} [join=null] Table name of joined target table to use in join query statement.
 * @param {String} [on=null] Constraint for to use table join.
 * @param {String|Array} [field=null] Fields to be used in query statement.
 * @param {String|Object} [where=null] Where condition to be used in query statement.
 * @param {String} [group=null] Group by clause to be used in query statement.
 * @param {String} [having=null] Having condition to be used in group by clause of query statement.
 * @param {String} [order=null] Order by clause to be used in query statement.
 * @param {Number} [limit=0] Number of rows to return to be used in query statement. If `0` no limit in used.
 * @throws 'Not passed table name to be used in query statement!'
 * @returns {Promise<Any>} Result of executing `SELECT` query statement using table join and group.
 */
const selectJoinGroup = async (
  table,
  type = null,
  join = null,
  on = null,
  field = null,
  where = null,
  group = null,
  having = null,
  order = null,
  limit = 0
) => {
  return await query(querySelectJoinGroup(table, type, join, on, field, where, group, having, order, limit))
}

/**
 * Returns result that single row of executing `SELECT` query statement.
 *
 * @param {String} table Table name to be used in `SELECT` query statement.
 * @param {String|Array} [field=null] Fields to be used in `SELECT` query statement.
 * @param {String|Object} [where=null] Where condition to be used in `SELECT` query statement.
 * @param {String} [order=null] Order by clause to be used in `SELECT` query statement.
 * @throws 'Not passed table name to be used in query statement!'
 * @returns {Object|null} Result that single row of executing `SELECT` query statement.
 */
const selectSingle = async(table, field = null, where = null, order = null) => {
  const rows = await query(querySelect(table, field, where, order))
  return rows[0] || null
}

/**
 * Returns result after executing `UPDATE` query statement.
 *
 * @param {String} table Table name to use in query statement.
 * @param {Object} values Values object that consisting of field names and values to be used in `UPDATE` query statement.
 * @param {String|Object} where Where condition to be used in query statement.
 * @throws 'Not passed table name to be used in query statement!'
 * @throws 'Not passed object consisting of field and value to be used in UPDATE query statement!'
 * @throws 'Not passed update condition clause to be used in UPDATE query statement!
 * @returns {Promise<Any>} Result of executing `UPDATE` query statement.
 */
const update = async (table, values, where) => {
  return await query(queryUpdate(table, values, where))
}

const ABMariaDB = {
  count,
  createPool,
  getConnection,
  insert,
  mariadb,
  query,
  select,
  selectGroup,
  selectJoin,
  selectJoinGroup,
  selectSingle,
  update
}


module.exports = ABMariaDB

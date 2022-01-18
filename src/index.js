const mariadb = require('mariadb')


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

  mariadb.log = config.logger || console
  mariadb.pool = mariadb.createPool(config)

  if (!mariadb.pool) {
    throw `[MariaDB createPool] MariaDB pool not created!`
  }

  return mariadb.pool
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
    mariadb.log.debug && mariadb.log.debug(`[MariaDB query] ${query}`)
    return await connection.query(query)
  } catch (error) {
    mariadb.log.error && mariadb.log.error(`[MariaDB query] error: %o`, error)
    throw error
  } finally {
    connection.release && connection.release()
    connection.end && connection.end()
  }
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
  if (mariadb.pool) {
    mariadb.connection = await mariadb.pool.getConnection()
    poolInfo()
    return mariadb.connection
  }

  if (mariadb.connection && mariadb.connection.isValid()) {
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

  mariadb.log = config.logger || console
  mariadb.connection = await mariadb.createConnection(config)

  if (!mariadb.connection) {
    throw `[MariaDB getConnection] MariaDB connection not created!`
  }

  return mariadb.connection
}

/**
 * MariaDB pool connection count information output.
 */
const poolInfo = () => {
  if (!mariadb.pool) return

  mariadb.log && mariadb.log.info(
    `[MariaDB pool] connections - ` +
    `active: ${mariadb.pool.activeConnections()} / ` +
    `idle: ${mariadb.pool.idleConnections()} / ` +
    `total: ${mariadb.pool.totalConnections()}`
  )
}

const ABMariaDB = { createPool, getConnection, mariadb, query }


module.exports = ABMariaDB

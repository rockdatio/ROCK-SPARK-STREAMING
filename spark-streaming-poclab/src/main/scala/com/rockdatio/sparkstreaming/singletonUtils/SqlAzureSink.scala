package com.rockdatio.sparkstreaming.singletonUtils

import org.apache.commons.dbcp2.BasicDataSource

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.TimeZone

class SqlAzureSink(getMysqlConnection: () => Connection, database: String) extends Serializable {
  lazy val clientSql: Connection = getMysqlConnection()
  lazy val statement: Statement = clientSql.createStatement()

  def closePS(rs: ResultSet): Boolean = {
    var result: Boolean = false
    if (rs.getBoolean("delivered")) result = true
    rs.close()
    result
  }

  def returnBool(returnValue: String): Boolean = if (returnValue == "1") true else false

  def evalRs(rs: ResultSet): Boolean = if (rs.next()) rs.getBoolean("delivered") else false

  def evalResultSet(rs: ResultSet): Boolean = if (rs.next()) true else false

  def evalDeliveredRs(rs: ResultSet): Boolean = if (rs.next()) closePS(rs) else false

  def getTokenDateRs(rs: ResultSet): String = if (rs.next()){
    rs.getString("Date")
  } else ""

  private def getCurrentDate(date: String): java.sql.Date = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    format.setTimeZone(TimeZone.getTimeZone("America/Bogota"))
    val currentDate: java.util.Date = format.parse(date)
    new java.sql.Date(currentDate.getTime)
  }

  def findByTarjeta2(table: String, schema: String, nrotarjeta: String): Boolean = {
    val statement4 = clientSql.createStatement()
    val result: ResultSet = statement4.executeQuery(s"SELECT delivered FROM [${schema}].${table} where debit_card_number = ${'"'}${nrotarjeta}${'"'}")
    //    statement4.close()
    evalRs(result)
  }

  def findTokenControlTable(table: String, schema: String): Boolean = {
    if (clientSql != null) {
      val statement5 = clientSql.createStatement()
      try {
        val rs: ResultSet = statement5.executeQuery(s"SELECT * FROM ${schema}.${table}")
        evalResultSet(rs)
      } finally {
        statement5.close()
      }
    } else {
      false
    }
  }

  def findByNrotarjeta(table: String, schema: String, nrotarjeta: String, date: String): Boolean = {
    val statement3 = clientSql.createStatement()
    try {
      val rs: ResultSet = statement3.executeQuery(s"SELECT delivered FROM ${schema}.${table} where debit_card_number ='${nrotarjeta}' and message_date = '${date}'")
      evalRs(rs)
    } finally {
      statement3.close()
    }
  }

  def insertIntoControlMessage(table: String, schema: String, date: String, nrotarjeta: String, delivered: String): Unit = {
    val statement2 = clientSql.createStatement()
    try statement2.executeUpdate(s"INSERT INTO ${schema}.${table} (message_date, debit_card_number, delivered) VALUES ('${date}', '${nrotarjeta}', '${delivered}')") finally statement2.close()
  }

  def findByCreditCard(table: String, schema: String, nrotarjeta: String, date: String): Boolean = {
    if (clientSql != null) {
      lazy val preparedStmt: PreparedStatement = clientSql.prepareStatement(s"SELECT delivered FROM ${schema}.${table} where debit_card_number = ? and message_date = ?")
      preparedStmt.setString(1, nrotarjeta)
      preparedStmt.setString(2, date)
      val rs: ResultSet = preparedStmt.executeQuery()
      evalDeliveredRs(rs)
    }
    else false
  }

  def findTokenByApplicationType(table: String,
                                 schema: String,
                                 applicationType: String): String = {
    if (clientSql != null) {
      lazy val preparedStmt: PreparedStatement = clientSql
        .prepareStatement(
          s"SELECT date FROM ${schema}.${table} where applicationType = ?"
        )
      preparedStmt.setString(1, applicationType)
      val rs: ResultSet = preparedStmt.executeQuery()
      getTokenDateRs(rs)
    }
    else ""
  }

  def insertIntoControlMessagePrep2(table: String,
                                    schema: String,
                                    date: String,
                                    nrotarjeta: String,
                                    delivered: String): Unit = {
    lazy val preparedStmt: PreparedStatement =
      clientSql.prepareStatement(s"INSERT INTO [${schema}].${table} (message_date, debit_card_number, delivered) VALUES (?, ?, ?)")
    preparedStmt.setDate(1, getCurrentDate(date))
    preparedStmt.setString(2, nrotarjeta)
    preparedStmt.setString(3, delivered)
    preparedStmt.executeUpdate()
    preparedStmt.close()
  }

  def insertIntoControlTokenTable(table: String,
                                  schema: String,
                                  currentTime: String,
                                  encryptedToken: String,
                                  applicationType: String): Unit = {
    println("INSERT-INTO-BEGIN")
    println("table----"+table)
    println("schema----"+schema)
    println("current time----"+currentTime)
    println("encryptedToken----"+encryptedToken)
    println("applicationType----"+applicationType)
    println("INSERT-INTO-END")
    if (clientSql != null) {
      lazy val preparedStmt: PreparedStatement = clientSql.prepareStatement(
        s"INSERT INTO [${schema}].${table} (process_name, token, token_time) VALUES (?, ?, ?)")

      preparedStmt.setString(1, applicationType)
      preparedStmt.setString(2, encryptedToken)
      preparedStmt.setString(3, currentTime)
      preparedStmt.executeUpdate()
      preparedStmt.close()
    }
  }

  def truncateTable(table: String, schema: String): Unit = {
    if (clientSql != null) {
      val statement2 = clientSql.createStatement()
      try statement2.executeUpdate(s"TRUNCATE TABLE ${schema}.${table}") finally statement2.close()
    }
  }

  def updateToken(table: String,
                  schema: String,
                  applicationType: String,
                  encryptedToken : String,
                  date:String): Unit = {
    if (clientSql != null) {
      val statement2 = clientSql.createStatement()
      try statement2.executeUpdate(
        s"UPDATE ${schema}.${table} where applicationType ='${applicationType}' SET date = '${date}' and token = '${encryptedToken}'") finally statement2.close()
    }
  }
}

object SqlAzureSink extends Serializable {
  def apply(url: String,
            driver: String,
            database: String,
            username: String,
            password: String): SqlAzureSink = {
    lazy val createMysqlConnection: () => Connection = () => {
      val connectionPool: BasicDataSource = new BasicDataSource()
      connectionPool.setDriverClassName(driver)
      connectionPool.setUrl(url)
      connectionPool.setDefaultQueryTimeout(360)
      connectionPool.setUsername(username)
      connectionPool.setPassword(password)
      //Set the connection pool size
      connectionPool.setInitialSize(20)
      //        Class.forName(driver)
      val connection: Connection = connectionPool.getConnection()
      //        val connection: Connection = DriverManager.getConnection(url, username, password)
      sys.addShutdownHook {
        connection.close()
      }
      connection
    }

    new SqlAzureSink(createMysqlConnection, database)
  }
}

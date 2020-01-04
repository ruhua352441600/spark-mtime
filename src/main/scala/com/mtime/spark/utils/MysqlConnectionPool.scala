package com.mtime.spark.utils

import java.sql.Connection
import java.util.Properties
import com.google.common.io.Resources
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.apache.log4j.Logger


/**
 * Created by Mtime on 2019/12/27.
 */
class MysqlConnectionPool {

}
object MysqlConnectionPool {
  //
  private val log: Logger = Logger.getLogger(this.getClass)
  val prop = loadConfig()
  private val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(prop.getProperty("db.url"))
      config.setUsername(prop.getProperty("db.username"))
      config.setPassword(prop.getProperty("db.password"))
      config.setLazyInit(true)
      config.setMinConnectionsPerPartition(8)
      config.setMaxConnectionsPerPartition(20)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        log.warn("Error in creation of connection pool" + exception.printStackTrace())
        None
    }
  }
  //
  private val connectionMtimeProPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(prop.getProperty("mtime_pro.db.url"))
      config.setUsername(prop.getProperty("mtime_pro.db.userName"))
      config.setPassword(prop.getProperty("mtime_pro.db.password"))
      config.setLazyInit(true)
      config.setMinConnectionsPerPartition(8)
      config.setMaxConnectionsPerPartition(20)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        log.warn("Error in creation of connection pool" + exception.printStackTrace())
        None
    }
  }
  //加载配置文件
  def loadConfig(): Properties = {
    val prop = new Properties()
    var fis = Thread.currentThread().getContextClassLoader.getResourceAsStream("./db.properties")
    if (fis == null) {
      fis = Resources.getResource("./conf/db.properties").openStream
    }
    prop.load(fis)
    prop
  }
  //
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }
  //
  def getMtimeProConnection: Option[Connection] = {
    connectionMtimeProPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }
  //
  def closeConnection(connection: Connection): Unit = {
    if (null != connection && !connection.isClosed) {
      connection.close()
    }
  }
}

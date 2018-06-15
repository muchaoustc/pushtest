package com.tencent.omg
import java.io.IOException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Date}
import java.util.function.Supplier

import org.apache.spark.SparkContext
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.{MissingNode, ObjectNode}

import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel._
import scala.concurrent.forkjoin.ForkJoinPool

package object utils /*extends Serializable with SimpleLogging*/ {
  private val date_formatter_cache = ThreadLocal.withInitial(new Supplier[mutable.Map[String, SimpleDateFormat]] {
    override def get() = mutable.Map[String, SimpleDateFormat]()
  })

  val COMMON_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  type MapTrait[K, V] = scala.collection.Map[K, V]

  type MutableMap[K, V] = scala.collection.mutable.Map[K, V]
  type MutableSet[T] = scala.collection.mutable.Set[T]
  type MutableArrayBuffer[T] = scala.collection.mutable.ArrayBuffer[T]

  private val object_mapper = new org.codehaus.jackson.map.ObjectMapper()

  def ifelse[T](cond: Boolean, positive: T, negative: T) = if (cond) positive else negative

  def mutableMap[K, V](elems: (K, V)*): MutableMap[K, V] = {
    scala.collection.mutable.Map(elems: _*)
  }

  def mutableSet[T](elems: T*): MutableSet[T] = {
    scala.collection.mutable.Set(elems: _*)
  }

  def mutableArrayBuffer[T](elems: T*): MutableArrayBuffer[T] = {
    scala.collection.mutable.ArrayBuffer(elems: _*)
  }

  implicit class TupleSeq[K, V](val seq: Seq[(K, V)]) extends AnyVal {
    def getOrElse(k: K, default: V): V = {
      var v = default

      var i = 0
      var found = false
      while (i < seq.size && !found) {
        val element: (K, V) = seq(i)
        if (element._1 == k) {
          found = true
          v = element._2
        }
        i += 1
      }

      v
    }

    def keys(): Iterable[K] = {
      seq.map(_._1)
    }

    def keySet(): Set[K] = {
      keys().toSet
    }
  }

  implicit class StringUtils(val underlying: String) extends AnyVal {
    def clearVersionCode(): String = {
      if (underlying.length >= 2)
        underlying.substring(0, underlying.length - 2) + "00"
      else
        underlying
    }

    def appendPath(component: String): String = {
      if (underlying.isEmpty)
        return component

      if (underlying.endsWith("/"))
        return underlying + component

      return underlying + "/" + component
    }

    def removePrefix(prefix: String): String = {
      if (underlying.startsWith(prefix))
        underlying.substring(prefix.length)
      else
        underlying
    }

    def appendPath(component: Int): String = appendPath(component.toString)

    import java.io._

    def writeToFile(file: File, encoding: String = "UTF-8"): Unit = {
      val writer = new PrintWriter(file, encoding)
      writer.write(underlying)
      writer.close()
    }

    def writeToPath(filePath: String, encoding: String = "UTF-8"): Unit = {
      writeToFile(new File(filePath), encoding)
    }

    def substringAfterDelim(delim: String, defaultValue: String): String = {
      if (isEmpty(underlying))
        return defaultValue

      val index = underlying.indexOf(delim)
      if (index == -1)
        return defaultValue

      return underlying.substring(index + 1)
    }

    def substringBeforeDelim(delim: String, defaultValue: String): String = {
      if (isEmpty(underlying))
        return defaultValue

      val index = underlying.indexOf(delim)
      if (index == -1)
        return defaultValue

      return underlying.substring(0, index)
    }

  }

  def murmur3Unsigned(s: String, seed: Int = 0): Long = {
    import com.google.common.base.Charsets
    import com.google.common.hash.Hashing
    import com.google.common.primitives.UnsignedInts
    if (s != null)
      UnsignedInts.toLong(Hashing.murmur3_32(seed).hashString(s, Charsets.UTF_8).asInt())
    else
      0
  }

  def md5Unsigned(s: String): Long = {
    import com.google.common.base.Charsets
    import com.google.common.hash.Hashing
    import com.google.common.primitives.UnsignedInts
    if (s != null)
      UnsignedInts.toLong(Hashing.md5().hashString(s, Charsets.UTF_8).asInt())
    else
      0
  }

  def kbBucket(s: String, maxBucket: Int = 100): Int = {
    return (kbBucketHash(s) % maxBucket).toInt
  }

  def kbBucketHash(s: String): Long = {
    import com.google.common.base.Charsets
    import com.google.common.hash.Hashing
    import com.google.common.primitives.UnsignedInts
    if (s != null) {
      val hash = toHexString(Hashing.md5().hashString(s, Charsets.UTF_8).asBytes()).substring(0, 5)
      return UnsignedInts.toLong(java.lang.Integer.parseInt(hash, 16))
    }
    else
      0
  }

  def toHexString(bytes: Array[Byte]): String = {
    val builder = mutable.StringBuilder.newBuilder

    bytes.foreach(b => {
      val hex = java.lang.Integer.toHexString(0xFF & b)
      if (hex.length == 1)
        builder.append('0')
      builder.append(hex)
    })

    return builder.toString
  }

  def murmur3Bucket(s: String, maxBucket: Int, seed: Int = 0): Int = {
    import com.google.common.math.LongMath

    return LongMath.mod(murmur3Unsigned(s, seed), maxBucket)
  }

  def phpBadHash(skey: String): Int = {
    if (skey == null) return 0
    var hash: Int = 0
    var i: Int = 0
    val len: Int = skey.length
    while (i < len) {
      val cc: Int = skey.charAt(i)
      hash += cc
      i += 1
    }
    hash &= 0x7FFFFFFF
    hash
  }

  implicit class ArrayUtils[T](val array: Array[T]) extends AnyVal {
    def getOrElse(index: Int, defaultValue: T): T = {
      if (index >= 0 && index < array.length)
        return array(index)
      else
        return defaultValue
    }

    def apply(index: Int, defaultValue: T): T = {
      if (index >= 0 && index < array.length)
        array(index)
      else
        defaultValue
    }

    def parallelized(concurrentJobNum: Int): scala.collection.parallel.mutable.ParArray[T] = {
      val par = array.par
      par.tasksupport = new ForkJoinTaskSupport(
        new ForkJoinPool(concurrentJobNum)
      )
      par
    }
  }

  implicit class SeqUtils[T](val seq: scala.collection.SeqLike[T, _]) extends AnyVal {
    def apply(index: Int, defaultValue: T): T = {
      if (index >= 0 && index < seq.length)
        seq(index)
      else
        defaultValue
    }

    def parallelized(concurrentJobNum: Int): scala.collection.parallel.ParSeq[T] = {
      val par = seq.par
      par.tasksupport = new ForkJoinTaskSupport(
        new ForkJoinPool(concurrentJobNum)
      )
      par
    }
  }

  implicit class MapUtils[K, V](val map: MapTrait[K, V]) extends AnyVal {
    def getInt(k: K, defaultInt: Int): Int = {
      if (map == null)
        return defaultInt

      val v = map.getOrElse(k, defaultInt)
      if (v.isInstanceOf[Int])
        return v.asInstanceOf[Int]
      else
        return safeToInt(v.toString, defaultInt)
    }

    def getDouble(k: K, defaultDouble: Double): Double = {
      if (map == null)
        return defaultDouble

      val v = map.getOrElse(k, defaultDouble)
      if (v.isInstanceOf[Double])
        return v.asInstanceOf[Double]
      else
        return safeToDouble(v.toString, defaultDouble)
    }

    def getString(k: K, defaultString: String): String = {
      if (map == null)
        return defaultString

      val v = map.getOrElse(k, defaultString)
      if (v.isInstanceOf[String])
        return v.asInstanceOf[String]
      else
        return "" + v
    }
  }

  def intToDate(date: Int): LocalDate = {
    val year = date / 10000
    val md = date - year * 10000
    val month = md / 100
    val day = md - month * 100

    LocalDate.of(year, month, day)
  }

  def nextDay(date: Int): Int = {
    val d = intToDate(date)
    val next = d.plusDays(1)
    return next.getYear * 10000 + next.getMonthValue * 100 + next.getDayOfMonth
  }

  def ftimeToIntDate(ftime: Long): Int = {
    import java.time._
    val dateTime = LocalDateTime.ofEpochSecond(ftime / 1000, 0, ZoneOffset.of("+08:00"))

    dateTime.getYear * 10000 + dateTime.getMonthValue * 100 + dateTime.getDayOfMonth
  }

  def daysInterval(d1: Int, d2: Int): Int = {
    (intToDate(d2).toEpochDay - intToDate(d1).toEpochDay).toInt
  }

  def addIntDouble(a: (Int, Double), b: (Int, Double)): (Int, Double) = (a._1 + b._1, a._2 + b._2)

  implicit class MutableIntValueMapUtils[T](val map: MutableMap[T, Int]) extends AnyVal {
    def add(k: T, v: Int, default: Int = 0): Int = {
      val n = map.getOrElse(k, default) + v
      map(k) = n
      n
    }

    def merge(other: scala.collection.Map[T, Int]): Unit = {
      other.foreach(p => {
        val n = map.getOrElse(p._1, 0) + p._2
        map(p._1) = n
        n
      })
    }
  }

  implicit class MutableDoubleValueMapUtils[T](val map: MutableMap[T, Double]) extends AnyVal {
    def add(k: T, v: Double, default: Double = 0): Double = {
      val n = map.getOrElse(k, default) + v
      map(k) = n
      n
    }

    def merge(other: scala.collection.Map[T, Double]): MutableMap[T, Double] = {
      other.foreach(p => {
        val n = map.getOrElse(p._1, 0.0) + p._2
        map(p._1) = n
        n
      })

      return map
    }

  }

  implicit class MutableMapUtils[K, V](val map: MutableMap[K, V]) extends AnyVal {
    def add(k: K, v: V, default: V, comb: (V, V) => V): V = {
      val n = comb(map.getOrElse(k, default), v)
      map(k) = n
      n
    }

    def merge(other: scala.collection.Map[K, V], default: V, comb: (V, V) => V): Unit = {
      other.foreach(p => {
        val n = comb(map.getOrElse(p._1, default), p._2)
        map(p._1) = n
        n
      })
    }
  }

  def measure[A](label: String)(r: => A): A = {
    val start = System.currentTimeMillis
    val ret = r
    println(s"Task '$label': ${
      System.currentTimeMillis() - start
    } ms")
    ret
  }

  def parseJsonString(s: String): org.codehaus.jackson.JsonNode = {
    object_mapper.readTree(s)
  }

  def parseJsonAs[T](s: String)(implicit mf: scala.reflect.Manifest[T]): T = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    parse(s).extract[T](DefaultFormats, mf)
  }

  def toJsonString[T <: AnyRef](t: T): String = {
    import org.json4s._

    implicit val formats = DefaultFormats
    org.json4s.jackson.Serialization.write[T](t)
  }

  implicit class JsonNodeUtils(val node: org.codehaus.jackson.JsonNode) extends AnyVal {

    import org.codehaus.jackson._

    def paths(paths: String*): JsonNode = {
      var n = node
      paths.foreach(p => n = n.path(p))
      n
    }

    def safeRemove(fieldName: String): JsonNode = {
      if (node != null && node.isInstanceOf[ObjectNode]) {
        val n = node.asInstanceOf[ObjectNode]
        val child = n.remove(fieldName)
        if (child != null)
          return child
      }

      return MissingNode.getInstance()
    }
  }

  def isEqual(a: Double, b: Double): Boolean = {
    Math.abs(a - b) <= EPSILON
  }


  def safeToInt(s: String, default: Int = -1): Int = {
    try {
      if (s == null || s.isEmpty)
        default
      else
        s.toInt
    } catch {
      case e: Throwable => default
    }
  }

  def safeToLong(s: String, default: Long = -1L): Long = {
    try {
      if (s == null || s.isEmpty)
        default
      else
        s.toLong
    } catch {
      case e: Throwable => default
    }
  }

  def safeToFloat(s: String, default: Float = -1.0F): Float = {
    try {
      if (s == null || s.isEmpty)
        default
      else
        s.toFloat
    } catch {
      case e: Throwable => default
    }

  }

  def safeToDouble(s: String, default: Double = -1.0): Double = {
    try {
      if (s == null || s.isEmpty)
        default
      else
        s.toDouble
    } catch {
      case e: Throwable => default
    }

  }

  def notnullOrElse[T](t: T, default: T): T = {
    if (t != null) t else default
  }

  def nonEmpty(s: String): Boolean = {
    s != null && s.length > 0
  }

  def isEmpty(s: String): Boolean = {
    !nonEmpty(s)
  }

  def nonEmpty[T <: TraversableOnce[_]](t: T): Boolean = {
    t != null && t.nonEmpty
  }

  def isEmpty[T <: TraversableOnce[_]](t: T): Boolean = {
    !nonEmpty(t)
  }

  def nonEmpty[T](t: Array[T]): Boolean = {
    t != null && t.nonEmpty
  }

  def isEmpty[T](t: Array[T]): Boolean = {
    !nonEmpty(t)
  }

  def mergeMutableMap[K, V](target: mutable.Map[K, V], source: scala.collection.Map[K, V], default: V, merger: (K, V, V) => V): Unit = {
    source.foreach(p => {
      val k = p._1
      val v = p._2

      val current = target.getOrElse(k, default)

      val newV = merger(k, current, v)

      target(k) = newV
    })
  }

  def isValidDeviceID(s: String): Boolean = {
    if (s == null || s.isEmpty)
      return false

    var i = 0
    var valid = true
    while (i < s.length && valid) {
      val c = s.charAt(i)
      valid = c == '_' || c == '-' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')

      i += 1
    }

    valid
  }

  def isValidCMSID(s: String): Boolean = {
    if (s == null || s.isEmpty)
      return false

    var i = 0
    var valid = true
    while (i < s.length && valid) {
      val c = s.charAt(i)
      valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
      i += 1
    }

    valid
  }

  def toBossLogDate(s: String, format: String = COMMON_DATE_FORMAT): Date = {
    if (s == null || s.isEmpty)
      return null

    val formatter = date_formatter_cache.get().getOrElseUpdate(format, {
      new SimpleDateFormat(format)
    })

    try {
      formatter.parse(s)
    } catch {
      case e: Throwable =>
        null
    }
  }

  def tryFinal(action: => Unit, disposal: => Unit) = {
    try {
      action
    }
    finally {
      try {
        disposal
      }
      catch {
        case _: IOException =>
      }
    }
  }

  type FuncCreateConnection = () => java.sql.Connection

  def createJDBCConnectionJob(configFile: java.io.File): FuncCreateConnection = {
    import java.io._
    import java.sql.DriverManager

    val properties = new java.util.Properties

    val input = new FileInputStream(configFile)

    tryFinal({
      properties.load(input)
    }, {
      input.close()
    })

    //jdbc:mysql://host:port/db_name?useUnicode=true&amp;characterEncoding=UTF-8
    val url = properties.getProperty("db_url")
    val driver = properties.getProperty("db_driver", "com.mysql.jdbc.Driver")
    val username = properties.getProperty("db_user")
    val pwd = properties.getProperty("db_pwd")

    return () => {
      Class.forName(driver).newInstance()
      DriverManager.getConnection(url, username, pwd)
    }
  }

  def createJDBCConnectionJob(sc: SparkContext, configFile: String): FuncCreateConnection = {
    import java.sql.DriverManager

    val configMap = sc.textFile(configFile).map(line => {
      val items = line.split("\t")
      if (items.size < 2)
        null
      else
        (items(0), items(1))
    }).filter(_ != null).collectAsMap()

    val url = configMap.getOrElse("db_url", "")
    val driver = configMap.getOrElse("db_driver", "com.mysql.jdbc.Driver")
    val username = configMap.getOrElse("db_user", "")
    val pwd = configMap.getOrElse("db_pwd", "")

    return () => {
      Class.forName(driver).newInstance()
      DriverManager.getConnection(url, username, pwd)
    }
  }

  val MAX_SIGNED_POWER_OF_TWO = 1 << (Integer.SIZE - 2)

  def ceilingPowerOfTwo(x: Int): Int = {
    if (x <= 0)
      2
    else {
      if (x > MAX_SIGNED_POWER_OF_TWO) {
        throw new ArithmeticException("ceilingPowerOfTwo(" + x + ") not representable as an int")
      }
      1 << -Integer.numberOfLeadingZeros(x - 1)
    }
  }

  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
}

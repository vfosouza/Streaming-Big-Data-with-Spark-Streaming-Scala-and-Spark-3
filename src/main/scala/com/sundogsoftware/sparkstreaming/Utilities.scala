package com.sundogsoftware.sparkstreaming

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher

object Utilities {
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials using twitter.txt in the data directory */
  def setupTwitter(): Unit = {
    import scala.io.Source
    import org.apache.http.client.methods.HttpPost
    import org.apache.http.impl.client.HttpClients
    import org.apache.http.util.EntityUtils
    import org.apache.http.message.BasicNameValuePair
    import org.apache.http.client.entity.UrlEncodedFormEntity
    import java.util.{ArrayList, Base64}
    import java.nio.charset.StandardCharsets

    val file = new java.io.File("data/twitter.txt")
    if (!file.exists()) throw new Exception("File data/twitter.txt not found")

    // Read credentials from file
    val lines = Source.fromFile(file).getLines().toList
    val key = lines.find(_.startsWith("consumerKey=")).map(_.split("=")(1)).orNull
    val secret = lines.find(_.startsWith("consumerSecret=")).map(_.split("=")(1)).orNull

    if (key == null || secret == null)
      throw new Exception("Twitter credentials not found in expected format")

    // For compatibility with existing Twitter4J code
    val accessToken = lines.find(_.startsWith("accessToken=")).map(_.split("=")(1)).orNull
    val accessTokenSecret = lines.find(_.startsWith("accessTokenSecret=")).map(_.split("=")(1)).orNull

    if (accessToken != null && accessTokenSecret != null) {
      System.setProperty("twitter4j.oauth.consumerKey", key)
      System.setProperty("twitter4j.oauth.consumerSecret", secret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    }

    // Generate and store bearer token for Twitter API v2
    val client = HttpClients.createDefault()
    val post = new HttpPost("https://api.twitter.com/oauth2/token")
    val credentials = Base64.getEncoder.encodeToString(s"$key:$secret".getBytes(StandardCharsets.UTF_8))

    post.addHeader("Authorization", s"Basic $credentials")
    post.addHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
    post.setEntity(new UrlEncodedFormEntity(new ArrayList[BasicNameValuePair]() {
      add(new BasicNameValuePair("grant_type", "client_credentials"))
    }))

    val response = client.execute(post)
    val responseString = EntityUtils.toString(response.getEntity, "UTF-8")

    val bearerToken = scala.util.parsing.json.JSON.parseFull(responseString) match {
      case Some(map: Map[String, Any]) => map("access_token").toString
      case _ => throw new Exception("Failed to obtain bearer token")
    }

    // Store the bearer token for use by other components
    System.setProperty("twitter.bearer.token", bearerToken)
  }
  
  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"                      
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
    val client = "(\\S+)"                     
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"              
    val request = "\"(.*?)\""                 
    val status = "(\\d{3})"
    val bytes = "(\\S+)"                     
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)    
  }
}
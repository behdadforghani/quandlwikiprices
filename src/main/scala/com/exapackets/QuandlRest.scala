package com.exapackets

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.client.ClientProtocolException;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.client.ClientResponse;
import scala.util.parsing.json._
import java.io._
import javax.ws.rs.core.MediaType


import java.util.zip.{ ZipInputStream, ZipOutputStream }
import scala.io.Source

import java.util.zip._
import scala.collection.mutable.ArrayBuffer



object QuandlRest {
  var quandlKey:String = null
  
  // Fixed part of Quandl WIKI prices URL
  val wikiUrl = "https://www.quandl.com/api/v3/datatables/WIKI/PRICES?qopts.export=true&api_key="
  
  // Name of the local extracted zip file
  val localZipFilename = "WIKI.csv.zip"
  
  def getZipEntryInputStream(zipFile: ZipFile)(entry: ZipEntry) = zipFile.getInputStream(entry)

  def httpGetString(url: String) = {
    val request = new ClientRequest(url)
    request.accept("application/json")
    val response = request.get(classOf[String])
    if (response.getStatus() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
        + response.getStatus()
        + "\nEntity: " + response.getEntity)
    }

    val bas = new ByteArrayInputStream(response.getEntity().getBytes())
    val string = org.apache.commons.io.IOUtils.toString(bas, "UTF-8")
    string
  }

  def httpGetBytes(url: String) = {
    val request = new ClientRequest(url)
    request.accept("*/*")
    val response = request.get(classOf[InputStream])
    if (response.getStatus() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
        + response.getStatus()
        + "\nEntity: " + response.getEntity)
    }

    val bas = response.getEntity
    val bytes = org.apache.commons.io.IOUtils.toByteArray(bas)
    bytes

  }

  def downloadData() {
        val dlLink = httpGetString(wikiUrl)
        val jd = JSON.parseFull(dlLink)
        val jsonDlLink = JSON.parseFull(dlLink)
    
        val datatable_bulk_download = jsonDlLink.get.asInstanceOf[Map[String, Any]]("datatable_bulk_download")
        val file = datatable_bulk_download.asInstanceOf[Map[String, Any]]("file")
        val link = file.asInstanceOf[Map[String, Any]]("link").asInstanceOf[String]
        val dlData = httpGetBytes(link)
        val bos = new BufferedOutputStream(new FileOutputStream(localZipFilename))
        bos.write(dlData)
        bos.close()    
  }
  
  def getZipEntries(source: String) = {
    val zipEntries = ArrayBuffer[(String, InputStream)]()
    val zipFile = new ZipFile(source)
    val entries = zipFile.entries()
    while(entries.hasMoreElements()) {
      val entry = entries.nextElement()
      val name = entry.getName
      val inputStream = zipFile.getInputStream( entry )
      zipEntries += ((name, inputStream))
    }

    zipEntries
  }

  def saveStream(name: String, inputStream: InputStream) {
    val outputStream = new FileOutputStream(new File(name))
    val BUFSIZE = 4096
    val buffer = new Array[Byte](BUFSIZE)
    var length:Integer = null
    while({length = inputStream.read(buffer); length } >= 0) {
    	outputStream.write(buffer, 0, length)
    }
    inputStream.close()
    outputStream.close()
    
  }
  
  def fetchData(key: String) : String = {
    val dlLink = httpGetString(wikiUrl+key)
    
    val jd = JSON.parseFull(dlLink)
    val jsonDlLink = JSON.parseFull(dlLink)

    val datatable_bulk_download = jsonDlLink.get.asInstanceOf[Map[String, Any]]("datatable_bulk_download")
    val file = datatable_bulk_download.asInstanceOf[Map[String, Any]]("file")
    val link = file.asInstanceOf[Map[String, Any]]("link").asInstanceOf[String]
    val dlData = httpGetBytes(link)
    val bos = new BufferedOutputStream(new FileOutputStream(localZipFilename))
    bos.write(dlData)
    bos.close()

    val zipEntries = getZipEntries(localZipFilename)
    if(zipEntries.size != 1) {
      throw new RuntimeException("More than one file exists in zip archive")
    }
    
    val (outFilename, zipStream) = zipEntries.head
    saveStream(outFilename, zipStream)
    
    outFilename
    
  }

}

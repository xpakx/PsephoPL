package io.github.xpakx.psephopl.utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.io.File;
import java.net.URISyntaxException;

import java.util.stream.Collectors;

public class CSVUtils 
{
  private static final String separator = ",";
  
  
  public static List<List<String>> readFile(String fileName)
  {    
    List<List<String>> result;
    try (BufferedReader br =  
      new BufferedReader(
      new FileReader(new File(CSVUtils.class.getClassLoader().getResource(fileName).getFile())))) 
    {
      result = parseFile(br);
    } 
    catch(IOException e)
    {
      result = null;
      System.out.println("ets");
    }
    return result;
  }
  
  public static List<List<String>> parseFile(BufferedReader fileReader) throws IOException
  {
    return fileReader
              .lines()
              .map(line -> parseLine(line))
              .collect(Collectors.toList());
  }
  
  public static List<String> parseLine(String line)
  {
    return parseLine(line, separator);
  }
  
  
  /* TODO should process double quotes in accordance with RFC 4180 
   * but there aren't any in datasets I use, so I ignored it 
   * in spite of simplicity
   */
  public static List<String> parseLine(String line, String customSeparator)
  {
    List<String> result = new ArrayList<String>();
    if(line != null && !line.isEmpty())
    {
      result = Arrays.asList(line.split(customSeparator));
    }
    return result;
  }
  
 
}

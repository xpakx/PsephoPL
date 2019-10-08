package io.github.xpakx.psephopl.utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.IOException;

public class CSVUtils 
{
  private static final String separator = ",";
  
  
  public static List<List<String>> parseFile(BufferedReader fileReader) throws IOException
  {
    return null;
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

package io.github.xpakx.psephopl.utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class CSVUtils 
{
 

  public static List<String> parseLine(String line)
  {
    List<String> result = new ArrayList<String>();
    if(line != null && !line.isEmpty())
    {
      result = Arrays.asList(line.split(","));
    }
    return result;
  }
  
  public static List<String> parseLine(String line, char customSeparator)
  {
    return null;
  }
  
 
}

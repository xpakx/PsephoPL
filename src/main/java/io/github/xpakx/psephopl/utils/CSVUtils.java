package io.github.xpakx.psephopl.utils;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class CSVUtils 
{
  private static final String separator = ",";

  public static List<String> parseLine(String line)
  {
    return parseLine(line, separator);
  }
  
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

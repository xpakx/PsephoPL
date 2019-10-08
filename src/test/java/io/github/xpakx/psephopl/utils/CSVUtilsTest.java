package io.github.xpakx.psephopl.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import org.mockito.Mock;
import org.mockito.InjectMocks;
import static org.mockito.BDDMockito.*;
import static org.junit.Assert.*;
import org.mockito.MockitoAnnotations;
import org.hamcrest.Matchers;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.Matchers.*;
import org.mockito.ArgumentCaptor;


import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;

public class CSVUtilsTest 
{
 

  @Test
  public void shouldReturnEmptyListIfLineEmpty() throws Exception 
  {
    //given
    String line = "";
    
    //when
    List<String> result = CSVUtils.parseLine(line);
    
    //then
    assertNotNull(result);
    assertThat(result.size(), is(0));
  }
  
  @Test
  public void shouldReturnEmptyListIfNullLine() throws Exception 
  {
    //given
    String line = null;
    
    //when
    List<String> result = CSVUtils.parseLine(line);
    
    //then
    assertNotNull(result);
    assertThat(result.size(), is(0));
  }
  
  //RFC 4180 2.4
  @Test
  public void shouldSplitLineByCommas()
  {
    //given
    String line = "one,two,three";
    
    //when
    List<String> result = CSVUtils.parseLine(line);
    
    //then
    assertNotNull(result);
    assertThat(result.size(), is(3));
    assertThat(result.get(0), is("one"));
    assertThat(result.get(1), is("two"));
    assertThat(result.get(2), is("three"));
  }
  
  @Test
  public void shouldSplitLineByCustomSeparator()
  {
    //given
    String line = "one;two;three";
    
    //when
    List<String> result = CSVUtils.parseLine(line, ";");
    
    //then
    assertNotNull(result);
    assertThat(result.size(), is(3));
    assertThat(result.get(0), is("one"));
    assertThat(result.get(1), is("two"));
    assertThat(result.get(2), is("three"));
  }
 
  @Test
  public void shouldContainsEmptyCells()
  {
    //given
    String line = "one,,three";
    String line2 = "one,,,,five";
    
    //when
    List<String> result = CSVUtils.parseLine(line);
    List<String> result2 = CSVUtils.parseLine(line2);
    
    //then
    assertNotNull(result);
    assertNotNull(result2);
    assertThat(result.size(), is(3));
    assertThat(result2.size(), is(5));
    assertThat(result.get(0), is("one"));
    assertThat(result.get(1), is(""));
    assertThat(result.get(2), is("three"));
  }
  
  @Test 
  public void shouldReadWholeFile() throws Exception
  {
    //given
    BufferedReader bufferedReader = mock(BufferedReader.class);
    when(bufferedReader.readLine())
    .thenReturn("one,two,three", "four,five,six", "seven,eight,nine");
    
    //when
    List<List<String>> result = CSVUtils.parseFile(bufferedReader);
    
    //then
    assertNotNull(result);
    assertThat(result.size(), is(3));
    assertNotNull(result.get(0));
    assertThat(result.get(0).size(), is(3));
    assertNotNull(result.get(1));
    assertThat(result.get(1).size(), is(3));
    assertNotNull(result.get(2));
    assertThat(result.get(2).size(), is(3));
  }
  
 
}

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
    assertThat(result.get(1), is("one"));
    assertThat(result.get(2), is("three"));
  }
  
  
 
}

package io.github.xpakx.psephopl.data;

import java.util.Set;

public class Gmina
{
  private final String teryt;
  private final String degruba;
  private final Integer votes;
  private final Set<Integer> votesForParties;
  
  public Gmina(String teryt, String degruba, Integer votes, Set<Integer> votesForParties)
  {
    this.teryt = teryt;
    this.degruba = degruba;
    this.votes = votes;
    this.votesForParties = votesForParties;
  }
  
  public String getTERYT()
  {
    return teryt;
  }
  
  public String getDEGRUBA()
  {
    return degruba;
  }
  
  public Integer getVotes()
  {
    return this.votes;
  }
  
  

}

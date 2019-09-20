## PsephoPL

I'm writing this to analyze election results data for the Poland. I'm mainly interested in some geographical correlations (e.g. between urbanization and voting for the Law and Justice party), basic alternative scenarios (e.g. what would happen if 100 voters of one party made different decision) and voting power defined as in Gelman et al. 2002.

Poland uses Jefferson-D'Hondt seat allocation method in parliamentary elections. It's believed to tend to favour the larger parties. 

Voting power is the probability that a single vote is decisive — that is without this vote results would be different. In practice that's equivalent to probability of electoral ties. Electoral ties are very rare empirically (Mulligan & Hunter 2001), and the same is true for ties in results of polish elections (Flis et al. 2019). I expect that there aren't any (or only a few) in the dataset from 2015, and won't be any (or only a few) in the dataset from 2019 elections. 

Recently Wilkinson (2019) proposed that urbanization processes produce spatial segregation that, in turn, causes political polarization. If that's true for Poland, we should see a strong correlation between urbanization measured by degree of urbanisation classification (DEGRUBA) and percent of votes for two biggest parties. I had a problem with finding the newest DEGRUBA dataset so mine is a bit outdated, but that's not a big issue because probably only a few administration units was reclasiffied. 


Flis, J., Słomczyński, W., & Stolicki, D. (2019). Pot and ladle: a formula for estimating the distribution of seats under the Jefferson–D’Hondt method. Public Choice. doi: 10.1007/s11127-019-00680-w

Gelman, A., Katz, J. N., & Tuerlinckx, F. (2002). The mathematics and statistics of voting power. Statistical Science, 17(4), 420–435. doi: 10.1214/ss/1049993201

Mulligan, C. B., & Hunter, C. G. (2003). The Empirical Frequency of a Pivotal Vote. Public Choice, 116(1/2), 31–54. doi: 10.1023/a:1024244329828

Wilkinson, W. (2019). The Density Divide: Urbanization, Polarization, and Populist Backlash. Washington: Niskanen Center.

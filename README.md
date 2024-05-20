### Overview
This is a hackathon project that calculates theoretical markout for Uniswap LPs on historical data.

The project was inspired by the paper [Learning from DeFi: Would Automated Market Makers Improve Equity Trading?](https://deliverypdf.ssrn.com/delivery.php?ID=361095067121094007068095019010002123098078055012042006031076074011117098024114099087045052103009119007115102119019003029075008109011088034000024070103015075066095127032038033078030002082103106101122126014108112027031030095082086095104071126127080028110&EXT=pdf&INDEX=TRUE) by Malinova and Park (2023). Intuitively, it seems like LPs (passive market makers) would be most profitable in low-volume markets where it’s not profitable to actively market make. There have been a lot of studies of LP profitability, but most limit their analysis to ETH-USDC and other high volume markets. My project attempted to conduct a markout analysis across a variety of low and high volume asset pairs to test the intuition. It built on a colleague's past LP markout analysis.

essentially I define three functions in markout.py:
1. `load_all_swaps` - takes in a pool address and chain, uses readFromMemoryOrDisk and puts the results into a dataframe.
2. `construct_markout` - takes in the data from #1, the decimals of the tokens, and whether WETH is token0 or not, and constructs markout and volume columns for each row (which is one transaction).
3. `execute_markout` - sums the volume and markout columns and outputs the result.

then in `analysis.ipynb` I:
1. create a `pools` DF from a CSV of 1300 pools where at least 1 side is WETH. All ETH L1
2. create a for loop through `pools` and loop all three markout.py functions for each pool, appending the markout and volume per pool to the results
4. plot

### Notes
1. I didn't get the analysis to work because I didn't have complete source data for `v3-polars.`
2. This analysis won't run natively on your computer. You need to load `v3-polars` into the same local directory then download all data to run this.

I will probably come back to this when I have some time, but also encourage you to help out :)

*These are my independent thoughts and do not necessarily represent the views of my employer.*

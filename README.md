### Overview
This is a hackathon project that calculates theoretical markout for Uniswap LPs on historical data.

The project was inspired by the paper [Learning from DeFi: Would Automated Market Makers Improve Equity Trading?](https://deliverypdf.ssrn.com/delivery.php?ID=361095067121094007068095019010002123098078055012042006031076074011117098024114099087045052103009119007115102119019003029075008109011088034000024070103015075066095127032038033078030002082103106101122126014108112027031030095082086095104071126127080028110&EXT=pdf&INDEX=TRUE) by Malinova and Park (2023). Intuitively, it seems like LPs (passive market makers) would be most profitable in low-volume markets where it’s not profitable to actively market make. There have been a lot of studies of LP profitability, but most limit their analysis to ETH-USDC and other high volume markets. My project attempted to conduct a markout analysis across a variety of low and high volume asset pairs to test the intuition. It built on a colleague's past LP markout analysis.

![chart showing decreasing benefit as volume increases](https://i.imgur.com/nrcNyON.png)
![chart showing decreasing benefit as volume increases](https://i.imgur.com/MjjeBha.png)

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

# update 5/19
Okay now I'm going to try this method lmao
1. Load all the swaps data for all 1300 pools into a single DataFrame.
2. Sort the DataFrame by pool address, block number, and log index using the sort method in Polars.
3. Calculate the markout price for each row by adding a time delta (e.g., 5 minutes) to the timestamp and finding the corresponding price at that timestamp. You can use the window function in Polars to perform this calculation efficiently.
4. Sum the markout and trading volume for each pool address using the groupby and agg methods in Polars.
5. Extract the summed markout and volume values for each pool address into a new DataFrame using the select method.
6. Plot the markout versus volume

# update 5/26
Something like:
- Load everything into a DataFrame
- Sort the DataFrame by pool address, block number, and log index
- Add pool price from sqrtPriceX96
- Add markout price based on pool price
- Add execution price based on token0/token1
- Add direction
- Add volume
- Calculate markout per-txn

- Cumsum markout and volume per-address
- Plot

``` python
# Calculate the markout price for each row
df = df.with_columns(
    pl.col("timestamp").add(pl.duration(minutes=5)).alias("markout_timestamp")
)
df = df.with_columns(
    pl.col("price").shift_and_fill(-1).over("pool_address").alias("markout_price")
)

# Sum the markout and trading volume for each pool address
pool_summary = df.groupby("pool_address").agg(
    [
        pl.sum("markout_price").alias("total_markout"),
        pl.sum("volume").alias("total_volume"),
    ]
)
```

# notes 5/26
- markout needs to be WETH-denominated?
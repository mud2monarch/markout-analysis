essentially I define three functions in markout.py:
1. `load_all_swaps` - takes in a pool address and chain, uses readFromMemoryOrDisk and puts the results into a DF
2. `construct_markout` - takes in the data from #1, a timeseries of WETH-USDC prices (from above), and three constants (decimals, and whether WETH is token0 or not) and constructs markout and volume columns
3. `execute_markout` - sums the volume and markout columns and outputs the result as a dict.

then in `analysis.ipynb` I:
1. create a pools DF from a CSV of 1300 pools where at least 1 side is WETH. All ETH L1
2. initialize the WETH_USDC 5 bps pool as a v3Pool and getPriceSeriesfrequency='1s'  on it
3. create a for loop through pools and loop all three markout.py functions for each pool, appending to results
4. plot
import polars as pl
from state import v3Pool
from datetime import datetime

def load_all_swaps(
        pool_address: str,
        chain: str
) -> pl.DataFrame:
    
    #TODO: add arbitrary date periods

    pool = v3Pool(pool_address.lower(), chain.lower())

    start_date = datetime(2022, 6, 1)
    end_date = datetime(2022, 9, 1)

    # put raw data into a folder called 'data' in the home of the directory
    df = (pool
        .readFromMemoryOrDisk(data='pool_swap_events', data_path='data')
        .filter(
            (pl.col('block_timestamp') >= start_date) &
            (pl.col('block_timestamp') < end_date)
        )
    )
    return df

def construct_markout(
        data: pl.DataFrame,
        IS_WETH_TOKEN0: bool,
        TOKEN0_DECIMALS: int,
        TOKEN1_DECIMALS: int
) -> pl.DataFrame:

    sqrtprice_list = data.select(pl.col("sqrtPriceX96")).to_series().to_list()
    med_price = [(int(i)/(2 ** 96))**2 for i in sqrtprice_list]

    df=(
        data.select(
            'block_timestamp',
            'block_number',
            'amount0',
            'amount1',
            'sqrtPriceX96',
            'liquidity',
            'as_of'
        )
        .sort(
            'as_of'
        ).with_columns(
            [
                pl.when(pl.col("amount1") < 0)
                .then(-1)
                .otherwise(1).alias('direction'),

                ((10**(TOKEN1_DECIMALS-TOKEN0_DECIMALS))/(pl.col('amount1')/pl.col('amount0'))).alias('execution_px'),
                # think i need this really cursed construction because i have been encountering an integer overflow from the following line. from grace.
                # ((10**(TOKEN1_DECIMALS-TOKEN0_DECIMALS))/((pl.col('sqrtPriceX96').cast(pl.Float64)/(2 ** 96)) ** 2)).cast(pl.Float64).alias('markout_px')])
                
                pl.Series(values=med_price, dtype=pl.Float64).alias('unadj_markout_px')
            ]
        ).with_columns(
            [
                ((10 ** (TOKEN1_DECIMALS-TOKEN0_DECIMALS)) / (pl.col('unadj_markout_px'))).alias('markout_px')
            ]
        ).with_columns(
            [
                pl.col('markout_px')
                    .last() # last value over...
                    .over(pl.col('block_timestamp').dt.truncate('5m')) # 5 mins worth of 'block_timestamp'
                    .shift(-1).alias('markout_px_5m'),
                pl.when(
                    pl.col('amount1') < 0,
                    ~IS_WETH_TOKEN0
                ).then(
                    pl.col('amount1').abs()/ 10 ** TOKEN1_DECIMALS
                ).when(
                    pl.col('amount1') < 0,
                    IS_WETH_TOKEN0
                ).then(
                    pl.col('amount1').abs() / 10**TOKEN1_DECIMALS / pl.col('execution_px')
                ).when(
                    pl.col('amount0') < 0,
                    IS_WETH_TOKEN0
                ).then(
                    pl.col('amount0').abs() / 10 ** TOKEN0_DECIMALS
                ).when(
                    pl.col('amount0') < 0,
                    ~IS_WETH_TOKEN0
                ).then(
                    pl.col('amount0').abs() / 10 ** TOKEN0_DECIMALS / pl.col('execution_px')
                ).otherwise(
                    pl.lit(None)
                ).alias('volume')
            ]
        ).with_columns(
            [
                (pl.col('direction') * pl.col('volume') * ((pl.col('markout_px_5m') - pl.col('execution_px')))).alias('markout')
            ]
        )
    )

    return df

def execute_markout(
    markout_df = pl.DataFrame
) -> dict:

    df = markout_df.select(
        pl.sum('volume').alias('total_volume'),
        pl.sum('markout').alias('total_markout')
    ).to_dict()

    return df
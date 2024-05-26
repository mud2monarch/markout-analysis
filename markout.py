import polars as pl
from datetime import datetime

def loadSwaps(
        filepath: str = 'all_swaps.csv',
        dt_format: str = '%Y-%m-%d %H:%M:%S %Z'
) -> pl.DataFrame:
    """
    @param filepath: pass the filepath as a relative or absolute path, including the extension.
    @dt_format: you can pass a custom dt format. This uses the default from Allium.

    Notice: table should follow the schema described in all_swaps.sql
    """
    
    df = (pl.read_csv(f'{filepath}', dtypes={
            'amount0':pl.Float64,
            'amount1':pl.Float64,
            'sqrtPriceX96':pl.String
            })
        .with_columns(
            pl.col('block_timestamp').str.strptime(pl.Datetime, format = f'{dt_format}')
        ).sort(
            ['address', 'block_number', 'transaction_index']
        )
    )
    
    return df

def addPrice(
        data: pl.DataFrame
) -> pl.DataFrame:
    """
    Helper function to add a new column with sqrtPriceX96 converted to price
    """
    """
    Austin TODO: i think you should also be able to apply map_batches
    
    Zach: I don't think this is possible actually because the problem is that you need to use native python dtypes.
    but maybe? not a priority.
    """
    sqrtPrice_list = data.select(pl.col("sqrt_price_x96")).to_series().to_list()
    price_list = [(int(i)/(2 ** 96))**2 for i in sqrtPrice_list]
    
    df = data.with_columns(pool_price = pl.Series(values=price_list, dtype=pl.Float64))
    
    return df

def construct_markout (
        data: pl.DataFrame,
) -> pl.DataFrame:
    
    df = addPrice(data)

    df = (
        df.join_asof(
            other = df.select(
                pl.col('block_timestamp').dt.offset_by('-5m').alias('timestamp_minus_five_min'),
                pl.col('pool_price'),
                pl.col('address')
            ),
            left_on='timestamp',
            right_on='timestamp_plus_five_min',
            by='address',
            strategy='backward'
        ).rename({'pool_price_right': 'markout_price'})
        .with_columns(
            execution_price = ((10 ** (pl.col('token1_decimals')-pl.col('TOKEN0_DECIMALS')))
                                /
                                (pl.col('amount1')/pl.col('amount0'))),
            volume = pl
                .when(pl.col('token0_symbol') == 'WETH') # if WETH is in token0 slot
                .then(pl.col('amount0').abs()/(10 ** pl.col('token0_decimals'))) # then volume (in WETH) = amount0, decimal-adjusted
                .otherwise(pl.col('amount1').abs()/(10 ** pl.col('token1_decimals'))), # otherwise, WETH is in token1 slot and volume (in WETH = amount1, decimal adjusted),
            direction = pl.when(pl.col("amount1") < 0)
                .then(-1)
                .otherwise(1).alias('direction')
        ).with_columns(
            markout = (pl.col('direction') * pl.col('volume') * ((pl.col('markout_price') - pl.col('execution_price'))))
        )
    )

    return df
# def construct_markout(
#         data: pl.DataFrame,
#         IS_WETH_TOKEN0: bool,
#         TOKEN0_DECIMALS: int,
#         TOKEN1_DECIMALS: int
# ) -> pl.DataFrame:

#     sqrtprice_list = data.select(pl.col("sqrtPriceX96")).to_series().to_list()
#     med_price = [(int(i)/(2 ** 96))**2 for i in sqrtprice_list]

#     df=(
#         data.select(
#             'block_timestamp',
#             'block_number',
#             'amount0',
#             'amount1',
#             'sqrtPriceX96',
#             'liquidity',
#             'as_of'
#         )
#         .sort(
#             'as_of'
#         ).with_columns(
#             [
#                 pl.when(pl.col("amount1") < 0)
#                 .then(-1)
#                 .otherwise(1).alias('direction'),

#                 ((10**(TOKEN1_DECIMALS-TOKEN0_DECIMALS))/(pl.col('amount1')/pl.col('amount0'))).alias('execution_px'),
#                 # think i need this really cursed construction because i have been encountering an integer overflow from the following line. from grace.
#                 # ((10**(TOKEN1_DECIMALS-TOKEN0_DECIMALS))/((pl.col('sqrtPriceX96').cast(pl.Float64)/(2 ** 96)) ** 2)).cast(pl.Float64).alias('markout_px')])
                
#                 pl.Series(values=med_price, dtype=pl.Float64).alias('unadj_markout_px')
#             ]
#         ).with_columns(
#             [
#                 ((10 ** (TOKEN1_DECIMALS-TOKEN0_DECIMALS)) / (pl.col('unadj_markout_px'))).alias('markout_px')
#             ]
#         ).with_columns(
#             [
#                 pl.col('markout_px')
#                     .last() # last value over...
#                     .over(pl.col('block_timestamp').dt.truncate('5m')) # 5 mins worth of 'block_timestamp'
#                     .shift(-1).alias('markout_px_5m'),
#                 pl.when(
#                     pl.col('amount1') < 0,
#                     ~IS_WETH_TOKEN0
#                 ).then(
#                     pl.col('amount1').abs()/ 10 ** TOKEN1_DECIMALS
#                 ).when(
#                     pl.col('amount1') < 0,
#                     IS_WETH_TOKEN0
#                 ).then(
#                     pl.col('amount1').abs() / 10**TOKEN1_DECIMALS / pl.col('execution_px')
#                 ).when(
#                     pl.col('amount0') < 0,
#                     IS_WETH_TOKEN0
#                 ).then(
#                     pl.col('amount0').abs() / 10 ** TOKEN0_DECIMALS
#                 ).when(
#                     pl.col('amount0') < 0,
#                     ~IS_WETH_TOKEN0
#                 ).then(
#                     pl.col('amount0').abs() / 10 ** TOKEN0_DECIMALS / pl.col('execution_px')
#                 ).otherwise(
#                     pl.lit(None)
#                 ).alias('volume')
#             ]
#         ).with_columns(
#             [
#                 (pl.col('direction') * pl.col('volume') * ((pl.col('markout_px_5m') - pl.col('execution_px')))).alias('markout')
#             ]
#         )
#     )

#     return df

def execute_markout(
    markout_df = pl.DataFrame
) -> dict:

    df = markout_df.select(
        pl.sum('volume').alias('total_volume'),
        pl.sum('markout').alias('total_markout')
    ).to_dict()

    return df
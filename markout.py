import polars as pl
from datetime import datetime

def loadSwaps(
        filepath: str = 'all_swaps.csv',
        dt_format: str = '%Y-%m-%d %H:%M:%S %Z'
) -> pl.DataFrame:
    """
    @param filepath: str // pass the filepath as a relative or absolute path, including the extension.
    @param dt_format: str // you can pass a custom dt format. The default is Allium's timestamp formatting.

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
    Helper function to add a new column with sqrtPriceX96 converted to price, and decimal adjusted
    """

    sqrtPrice_list = data.select(pl.col('sqrtPriceX96')).to_series().to_list()
    price_list = [(int(i)/(2 ** 96))**2 for i in sqrtPrice_list]
    
    # this returns the 'multiplicative inverse' of the pool price
    # this is the right price to correspond with `execution_price = (10^(decimal1-decimal0))/(amount1/amount0)`.
    # I think this is pretty jacked, but it should work.
    # TODO: fix.

    df = data.with_columns(
        pool_price = 1/((pl.Series(values=price_list, dtype=pl.Float64))/(10 ** (pl.col('token1_decimals')-pl.col('token0_decimals'))))
    )
    
    return df

def construct_markout (
        data: pl.DataFrame,
        timedelta: str = '5m'
) -> pl.DataFrame:
    """
    @param data: pl.DataFrame // your Polars dataframe from loadSwaps()
    @param timedelta: str // the markout timedelta you'd like to use, formatted per the documentation in `polars.Expr.dt.offset_by`

    Function that adds all markout columns (execution_price, markout_price, direction, and volume) per-row.
    """

    df = addPrice(data)

    df = (
        df.join_asof(
            other = df.select(
                pl.col('block_timestamp').dt.offset_by(f'-{timedelta}').alias('timestamp_minus_five_min'),
                pl.col('pool_price'),
                pl.col('address')
            ),
            left_on='block_timestamp',
            right_on='timestamp_minus_five_min',
            by='address',
            strategy='backward'
        ).rename({'pool_price_right': 'markout_price'})
        .with_columns(
            execution_price = ((10 ** (pl.col('token1_decimals')-pl.col('token0_decimals')))
                                /
                                (pl.col('amount1').abs()/pl.col('amount0').abs())),
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
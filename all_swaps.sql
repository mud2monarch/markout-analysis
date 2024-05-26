SELECT
-- cols are renamed to match v3-polars `swaps` schema
    liquidity_pool_address as address, -- the contract address of the pool; from Allium it comes in all lowercase, but it shouldn't matter (I think)
    block_timestamp, -- from Allium this comes in %Y-%m-%d %H:%M:%S %Z
    block_number,
    log_index as transaction_index, -- you need this in order to sort intra-block trades
    transaction_hash, -- for debugging. TODO: remove this column.
    token0_amount_raw_str as amount0, -- using the string value for max precision.
    token0_decimals,
    token1_amount_raw_str as amount1,-- using the string value for max precision.
    token1_decimals,
    sqrt_price_x96 as sqrtPriceX96, -- required. comes as a string.
    token0_symbol -- to identify WETH location
FROM `uniswap-labs.allium_ethereum.dex_uniswap_v3_protocol_liquidity_pool_events`
WHERE liquidity_pool_address in (
  SELECT
    LIQUIDITY_POOL_ADDRESS
  FROM `uniswap-labs.allium_ethereum.dex_trades`
  WHERE block_timestamp >= CAST('2023-06-01' AS TIMESTAMP)
    AND block_timestamp < CAST('2023-09-01' AS TIMESTAMP)
    AND protocol = 'uniswap_v3'
    and project = 'uniswap'
    and (token_sold_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' or token_bought_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
  GROUP BY LIQUIDITY_POOL_ADDRESS
  HAVING SUM(usd_amount) >= 30000
)
  and block_timestamp >= CAST('2023-06-01' AS TIMESTAMP)
  AND block_timestamp < CAST('2023-09-01' AS TIMESTAMP)
  and event = 'swap'
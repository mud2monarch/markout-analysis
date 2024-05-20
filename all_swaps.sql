SELECT
    liquidity_pool_address,
    block_timestamp,
    block_number,
    log_index,
    transaction_hash,
    token0_amount,
    token0_amount_raw,
    token0_decimals,
    token1_amount,
    token1_amount_raw,
    token1_decimals,
    sqrt_price_x96
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
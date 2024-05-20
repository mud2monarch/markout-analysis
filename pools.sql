-- this is queried from our GBQ instance, but it's the unmodified Allium tables.

SELECT
  distinct LIQUIDITY_POOL_ADDRESS,
  POOL_NAME,
  TOKEN0_ADDRESS,
  TOKEN0_DECIMALS,
  TOKEN1_ADDRESS,
  TOKEN1_DECIMALS,
  FEE
FROM `uniswap-labs.allium_ethereum.dex_uniswap_v3_protocol_liquidity_pool_events`
WHERE LIQUIDITY_POOL_ADDRESS IN (
  SELECT
    LIQUIDITY_POOL_ADDRESS
  FROM `uniswap-labs.allium_ethereum.dex_trades`
  WHERE block_timestamp >= CAST('2023-06-01' AS TIMESTAMP)
    AND block_timestamp < CAST('2023-09-01' AS TIMESTAMP)
    AND protocol = 'uniswap_v3'
    and project = 'uniswap'
  GROUP BY LIQUIDITY_POOL_ADDRESS
  HAVING SUM(usd_amount) >= 30000
)
  and block_timestamp >= CAST('2023-06-01' AS TIMESTAMP)
  AND block_timestamp < CAST('2023-09-01' AS TIMESTAMP)
-- query to test my code on the ETH-USDC pools to see if my code generates the same results as 0xfbifemboy

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
WHERE block_timestamp >= CAST('2021-08-01' AS TIMESTAMP)
  AND block_timestamp < CAST('2022-09-01' AS TIMESTAMP)
  and LIQUIDITY_POOL_ADDRESS in
    (lower('0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'), --eth/usdc 5 bps
    lower('0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8'), --eth/usdc 30 bps
    lower('0x7BeA39867e4169DBe237d55C8242a8f2fcDcc387')) --eth/usdc 100 bps
  and event = 'swap'
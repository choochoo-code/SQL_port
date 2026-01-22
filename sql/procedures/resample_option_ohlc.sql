CREATE DEFINER=`root`@`localhost` PROCEDURE `resample_option_ohlc`(
    IN p_src_table VARCHAR(64),
    IN p_dst_table VARCHAR(64),
    IN p_tf_minutes INT
)
BEGIN
    SET @sql = CONCAT(
    'INSERT INTO ', p_dst_table, '
    (StrikePrice, ContractType, ExpiryDate, Timestamp, Open, Close, High, Low)

    WITH base AS (
        SELECT
            StrikePrice,
            ContractType,
            ExpiryDate,
            Timestamp,
            Open,
            Close,
            High,
            Low,

            FLOOR(
              TIMESTAMPDIFF(
                MINUTE,
                CONCAT(DATE(Timestamp), '' 09:30:00''),
                Timestamp
              ) / ', p_tf_minutes, '
            ) AS bucket_id

        FROM ', p_src_table, '
        WHERE TIME(Timestamp) BETWEEN ''09:30:00'' AND ''15:57:00''
    ),

    bucketed AS (
        SELECT
            StrikePrice,
            ContractType,
            ExpiryDate,

            DATE_ADD(
              CONCAT(DATE(Timestamp), '' 09:30:00''),
              INTERVAL bucket_id * ', p_tf_minutes, ' MINUTE
            ) AS candle_ts,

            Timestamp,
            Open,
            Close,
            High,
            Low
        FROM base
    )

    SELECT
        StrikePrice,
        ContractType,
        ExpiryDate,
        candle_ts AS Timestamp,

        SUBSTRING_INDEX(
          GROUP_CONCAT(Open ORDER BY Timestamp),
          '','', 1
        ) AS Open,

        SUBSTRING_INDEX(
          GROUP_CONCAT(Close ORDER BY Timestamp DESC),
          '','', 1
        ) AS Close,

        MAX(High) AS High,
        MIN(Low)  AS Low

    FROM bucketed
    GROUP BY
        StrikePrice,
        ContractType,
        ExpiryDate,
        candle_ts
    ORDER BY
        StrikePrice,
        ContractType,
        ExpiryDate,
        candle_ts'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END
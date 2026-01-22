CREATE TABLE qqq_2024.call_orig_4h (
    StrikePrice INT NOT NULL,
    ContractType VARCHAR(10) NOT NULL,
    ExpiryDate DATETIME NOT NULL,
    Timestamp DATETIME NOT NULL,

    Open  DECIMAL(10,4),
    Close DECIMAL(10,4),
    High  DECIMAL(10,4),
    Low   DECIMAL(10,4),

    PRIMARY KEY (StrikePrice, ContractType, ExpiryDate, Timestamp)
);

CALL qqq_2024.resample_option_ohlc(
  'qqq_2024.call_orig',
  'qqq_2024.call_orig_4h',
  240
);
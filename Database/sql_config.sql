-- 切换到目标数据库
USE Quant;
GO

-- 创建文件组 FG2024 至 FG2028
ALTER DATABASE Quant
ADD FILEGROUP FG2024;
GO

ALTER DATABASE Quant
ADD FILEGROUP FG2025;
GO

ALTER DATABASE Quant
ADD FILEGROUP FG2026;
GO

ALTER DATABASE Quant
ADD FILEGROUP FG2027;
GO

ALTER DATABASE Quant
ADD FILEGROUP FG2028;
GO

-- 添加数据文件到 FG2024
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2024',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2024.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- 已提高一倍
    FILEGROWTH = 200MB
) TO FILEGROUP FG2024;
GO

-- 添加数据文件到 FG2025
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2025',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2025.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- 已提高一倍
    FILEGROWTH = 200MB
) TO FILEGROUP FG2025;
GO

-- 添加数据文件到 FG2026
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2026',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2026.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- 已提高一倍
    FILEGROWTH = 200MB
) TO FILEGROUP FG2026;
GO

-- 添加数据文件到 FG2027
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2027',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2027.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- 已提高一倍
    FILEGROWTH = 200MB
) TO FILEGROUP FG2027;
GO

-- 添加数据文件到 FG2028
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2028',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2028.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- 已提高一倍
    FILEGROWTH = 200MB
) TO FILEGROUP FG2028;
GO

-- 创建新的分区函数 pf_Yearly_New，覆盖 2024 年至 2028 年
CREATE PARTITION FUNCTION pf_Yearly_New (DATE)
AS RANGE LEFT FOR VALUES (
    ('20240101'), 
    ('20250101'), 
    ('20260101'), 
    ('20270101'), 
    ('20280101')
);
GO

-- 创建新的分区方案 ps_Yearly_New，映射到新的文件组
CREATE PARTITION SCHEME ps_Yearly_New
AS PARTITION pf_Yearly_New
TO (
    FG2024,
    FG2025,
    FG2026,
    FG2027,
    FG2028,
    [PRIMARY]  -- 对于2028年及以后的数据
);
GO

-- 创建分区表 StockPriceHistory 使用新的分区方案 ps_Yearly_New
CREATE TABLE StockPriceHistory (
    股票名称 NVARCHAR(100),
    股票代码 NVARCHAR(50),
    日期 DATE,
    开盘 FLOAT,
    收盘 FLOAT,
    最高 FLOAT,
    最低 FLOAT,
    成交量 FLOAT,
    成交额 FLOAT,
    振幅 FLOAT,
    涨跌幅 FLOAT,
    涨跌额 FLOAT,
    换手率 FLOAT,
    MA5 FLOAT,
    MA10 FLOAT,
    MA20 FLOAT,
    MA30 FLOAT,
    MA60 FLOAT,
    VOL5 FLOAT,
    VOL10 FLOAT,
    VOL20 FLOAT,
    VOL30 FLOAT,
    VOL60 FLOAT,
    PRIMARY KEY (股票代码, 日期)
) ON ps_Yearly_New (日期);
GO

-- 创建分区表 SectorPriceHistory 使用分区方案 ps_Yearly_New
CREATE TABLE SectorPriceHistory (
    股票名称 NVARCHAR(100),
    股票代码 NVARCHAR(50),
    日期 DATE,
    开盘 FLOAT,
    收盘 FLOAT,
    最高 FLOAT,
    最低 FLOAT,
    成交量 FLOAT,
    成交额 FLOAT,
    振幅 FLOAT,
    涨跌幅 FLOAT,
    涨跌额 FLOAT,
    换手率 FLOAT,
    MA5 FLOAT,
    MA10 FLOAT,
    MA20 FLOAT,
    MA30 FLOAT,
    MA60 FLOAT,
    VOL5 FLOAT,
    VOL10 FLOAT,
    VOL20 FLOAT,
    VOL30 FLOAT,
    VOL60 FLOAT,
    PRIMARY KEY (股票代码, 日期)
) ON ps_Yearly_New (日期);
GO

-- 创建分区表 ConceptPriceHistory 使用分区方案 ps_Yearly_New
CREATE TABLE ConceptPriceHistory (
    概念名称 NVARCHAR(100),
    股票代码 NVARCHAR(50),
    日期 DATE,
    开盘 FLOAT,
    收盘 FLOAT,
    最高 FLOAT,
    最低 FLOAT,
    成交量 FLOAT,
    成交额 FLOAT,
    振幅 FLOAT,
    涨跌幅 FLOAT,
    涨跌额 FLOAT,
    换手率 FLOAT,
    MA5 FLOAT,
    MA10 FLOAT,
    MA20 FLOAT,
    MA30 FLOAT,
    MA60 FLOAT,
    VOL5 FLOAT,
    VOL10 FLOAT,
    VOL20 FLOAT,
    VOL30 FLOAT,
    VOL60 FLOAT,
    PRIMARY KEY (股票代码, 日期)
) ON ps_Yearly_New (日期);
GO
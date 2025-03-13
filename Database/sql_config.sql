-- �л���Ŀ�����ݿ�
USE Quant;
GO

-- �����ļ��� FG2024 �� FG2028
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

-- ��������ļ��� FG2024
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2024',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2024.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- �����һ��
    FILEGROWTH = 200MB
) TO FILEGROUP FG2024;
GO

-- ��������ļ��� FG2025
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2025',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2025.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- �����һ��
    FILEGROWTH = 200MB
) TO FILEGROUP FG2025;
GO

-- ��������ļ��� FG2026
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2026',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2026.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- �����һ��
    FILEGROWTH = 200MB
) TO FILEGROUP FG2026;
GO

-- ��������ļ��� FG2027
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2027',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2027.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- �����һ��
    FILEGROWTH = 200MB
) TO FILEGROUP FG2027;
GO

-- ��������ļ��� FG2028
ALTER DATABASE Quant
ADD FILE (
    NAME = 'StockData2028',
    FILENAME = 'D:\Program Files\Microsoft SQL Server\StockData2028.ndf',
    SIZE = 200MB,
    MAXSIZE = 2048MB,  -- �����һ��
    FILEGROWTH = 200MB
) TO FILEGROUP FG2028;
GO

-- �����µķ������� pf_Yearly_New������ 2024 ���� 2028 ��
CREATE PARTITION FUNCTION pf_Yearly_New (DATE)
AS RANGE LEFT FOR VALUES (
    ('20240101'), 
    ('20250101'), 
    ('20260101'), 
    ('20270101'), 
    ('20280101')
);
GO

-- �����µķ������� ps_Yearly_New��ӳ�䵽�µ��ļ���
CREATE PARTITION SCHEME ps_Yearly_New
AS PARTITION pf_Yearly_New
TO (
    FG2024,
    FG2025,
    FG2026,
    FG2027,
    FG2028,
    [PRIMARY]  -- ����2028�꼰�Ժ������
);
GO

-- ���������� StockPriceHistory ʹ���µķ������� ps_Yearly_New
CREATE TABLE StockPriceHistory (
    ��Ʊ���� NVARCHAR(100),
    ��Ʊ���� NVARCHAR(50),
    ���� DATE,
    ���� FLOAT,
    ���� FLOAT,
    ��� FLOAT,
    ��� FLOAT,
    �ɽ��� FLOAT,
    �ɽ��� FLOAT,
    ��� FLOAT,
    �ǵ��� FLOAT,
    �ǵ��� FLOAT,
    ������ FLOAT,
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
    PRIMARY KEY (��Ʊ����, ����)
) ON ps_Yearly_New (����);
GO

-- ���������� SectorPriceHistory ʹ�÷������� ps_Yearly_New
CREATE TABLE SectorPriceHistory (
    ��Ʊ���� NVARCHAR(100),
    ��Ʊ���� NVARCHAR(50),
    ���� DATE,
    ���� FLOAT,
    ���� FLOAT,
    ��� FLOAT,
    ��� FLOAT,
    �ɽ��� FLOAT,
    �ɽ��� FLOAT,
    ��� FLOAT,
    �ǵ��� FLOAT,
    �ǵ��� FLOAT,
    ������ FLOAT,
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
    PRIMARY KEY (��Ʊ����, ����)
) ON ps_Yearly_New (����);
GO

-- ���������� ConceptPriceHistory ʹ�÷������� ps_Yearly_New
CREATE TABLE ConceptPriceHistory (
    �������� NVARCHAR(100),
    ��Ʊ���� NVARCHAR(50),
    ���� DATE,
    ���� FLOAT,
    ���� FLOAT,
    ��� FLOAT,
    ��� FLOAT,
    �ɽ��� FLOAT,
    �ɽ��� FLOAT,
    ��� FLOAT,
    �ǵ��� FLOAT,
    �ǵ��� FLOAT,
    ������ FLOAT,
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
    PRIMARY KEY (��Ʊ����, ����)
) ON ps_Yearly_New (����);
GO
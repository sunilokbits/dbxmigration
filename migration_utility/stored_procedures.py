"""
Complex SQL Stored Procedures for Migration to Databricks PySpark
"""

STORED_PROCEDURES = {
    "SP_SalesAggregation_Analytics": {
        "name": "SP_SalesAggregation_Analytics",
        "description": "Sales aggregation with CTEs, window functions, and multi-table joins",
        "code": """
CREATE PROCEDURE [dbo].[SP_SalesAggregation_Analytics]
    @StartDate   DATE,
    @EndDate     DATE,
    @RegionCode  NVARCHAR(10) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- CTE: Base Sales Data
    WITH BaseSales AS (
        SELECT
            s.SaleID,
            s.CustomerID,
            s.ProductID,
            s.SaleDate,
            s.Quantity,
            s.UnitPrice,
            s.Discount,
            (s.Quantity * s.UnitPrice * (1 - ISNULL(s.Discount, 0))) AS NetRevenue,
            c.Region,
            c.CustomerSegment,
            p.CategoryID,
            p.ProductName
        FROM dbo.Sales s
        INNER JOIN dbo.Customers c ON s.CustomerID = c.CustomerID
        INNER JOIN dbo.Products  p ON s.ProductID  = p.ProductID
        WHERE s.SaleDate BETWEEN @StartDate AND @EndDate
          AND (@RegionCode IS NULL OR c.Region = @RegionCode)
          AND s.IsActive = 1
    ),
    -- CTE: Monthly Aggregation
    MonthlySales AS (
        SELECT
            YEAR(SaleDate)            AS SaleYear,
            MONTH(SaleDate)           AS SaleMonth,
            Region,
            CustomerSegment,
            CategoryID,
            SUM(NetRevenue)           AS TotalRevenue,
            COUNT(DISTINCT SaleID)    AS TotalTransactions,
            COUNT(DISTINCT CustomerID)AS UniqueCustomers,
            AVG(NetRevenue)           AS AvgOrderValue,
            SUM(Quantity)             AS TotalUnits
        FROM BaseSales
        GROUP BY
            YEAR(SaleDate), MONTH(SaleDate),
            Region, CustomerSegment, CategoryID
    ),
    -- CTE: Running Totals & Rankings
    RankedSales AS (
        SELECT
            *,
            SUM(TotalRevenue) OVER (
                PARTITION BY Region, SaleYear
                ORDER BY SaleMonth
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS YTD_Revenue,
            RANK() OVER (
                PARTITION BY SaleYear, SaleMonth
                ORDER BY TotalRevenue DESC
            ) AS RegionRank,
            LAG(TotalRevenue, 1, 0) OVER (
                PARTITION BY Region, CustomerSegment
                ORDER BY SaleYear, SaleMonth
            ) AS PrevMonthRevenue
        FROM MonthlySales
    )
    -- Final Select with Growth Metrics
    SELECT
        SaleYear,
        SaleMonth,
        Region,
        CustomerSegment,
        CategoryID,
        TotalRevenue,
        TotalTransactions,
        UniqueCustomers,
        AvgOrderValue,
        TotalUnits,
        YTD_Revenue,
        RegionRank,
        PrevMonthRevenue,
        CASE
            WHEN PrevMonthRevenue = 0 THEN NULL
            ELSE ROUND(((TotalRevenue - PrevMonthRevenue) / PrevMonthRevenue) * 100, 2)
        END AS MoM_GrowthPct
    INTO #SalesResult
    FROM RankedSales
    ORDER BY SaleYear, SaleMonth, RegionRank;

    -- Insert into audit log
    INSERT INTO dbo.AuditLog (ProcedureName, ExecutedAt, RowsAffected, Parameters)
    VALUES (
        'SP_SalesAggregation_Analytics',
        GETDATE(),
        @@ROWCOUNT,
        CONCAT('StartDate=', @StartDate, ', EndDate=', @EndDate, ', Region=', ISNULL(@RegionCode,'ALL'))
    );

    SELECT * FROM #SalesResult;
    DROP TABLE #SalesResult;
END
"""
    },

    "SP_Inventory_Management": {
        "name": "SP_Inventory_Management",
        "description": "Inventory reorder management with cursors, temp tables, and error handling",
        "code": """
CREATE PROCEDURE [dbo].[SP_Inventory_Management]
    @WarehouseID   INT,
    @DryRun        BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Temp table: Low stock items
        CREATE TABLE #LowStockItems (
            ProductID       INT,
            ProductName     NVARCHAR(200),
            CurrentStock    INT,
            ReorderPoint    INT,
            ReorderQty      INT,
            LeadTimeDays    INT,
            SupplierID      INT,
            UnitCost        DECIMAL(18,4),
            ReorderStatus   NVARCHAR(50),
            CalcReorderQty  INT
        );

        -- Populate low stock
        INSERT INTO #LowStockItems
        SELECT
            p.ProductID,
            p.ProductName,
            i.QuantityOnHand,
            p.ReorderPoint,
            p.ReorderQuantity,
            s.LeadTimeDays,
            ps.SupplierID,
            ps.UnitCost,
            'PENDING',
            CASE
                WHEN i.QuantityOnHand <= 0             THEN p.ReorderQuantity * 2
                WHEN i.QuantityOnHand < p.ReorderPoint * 0.5  THEN p.ReorderQuantity + (p.ReorderPoint - i.QuantityOnHand)
                ELSE p.ReorderQuantity
            END
        FROM dbo.Products p
        INNER JOIN dbo.Inventory     i  ON p.ProductID = i.ProductID AND i.WarehouseID = @WarehouseID
        INNER JOIN dbo.ProductSupplier ps ON p.ProductID = ps.ProductID AND ps.IsPrimary = 1
        INNER JOIN dbo.Suppliers      s  ON ps.SupplierID = s.SupplierID
        WHERE i.QuantityOnHand <= p.ReorderPoint
          AND p.IsDiscontinued = 0
          AND s.IsActive = 1;

        -- Cursor: Process each reorder
        DECLARE @ProductID    INT,
                @SupplierID   INT,
                @ReorderQty   INT,
                @UnitCost     DECIMAL(18,4),
                @OrderTotal   DECIMAL(18,4),
                @PurchaseOrderID INT;

        DECLARE reorder_cursor CURSOR FAST_FORWARD FOR
            SELECT ProductID, SupplierID, CalcReorderQty, UnitCost
            FROM #LowStockItems;

        OPEN reorder_cursor;
        FETCH NEXT FROM reorder_cursor INTO @ProductID, @SupplierID, @ReorderQty, @UnitCost;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @OrderTotal = @ReorderQty * @UnitCost;

            IF @DryRun = 0
            BEGIN
                -- Create Purchase Order
                INSERT INTO dbo.PurchaseOrders (SupplierID, WarehouseID, OrderDate, Status, TotalAmount)
                VALUES (@SupplierID, @WarehouseID, GETDATE(), 'SUBMITTED', @OrderTotal);

                SET @PurchaseOrderID = SCOPE_IDENTITY();

                INSERT INTO dbo.PurchaseOrderLines (PurchaseOrderID, ProductID, Quantity, UnitCost, LineTotal)
                VALUES (@PurchaseOrderID, @ProductID, @ReorderQty, @UnitCost, @OrderTotal);

                UPDATE #LowStockItems
                SET ReorderStatus = 'ORDERED'
                WHERE ProductID = @ProductID;
            END
            ELSE
            BEGIN
                UPDATE #LowStockItems
                SET ReorderStatus = 'DRY_RUN'
                WHERE ProductID = @ProductID;
            END

            FETCH NEXT FROM reorder_cursor INTO @ProductID, @SupplierID, @ReorderQty, @UnitCost;
        END

        CLOSE reorder_cursor;
        DEALLOCATE reorder_cursor;

        COMMIT TRANSACTION;

        SELECT * FROM #LowStockItems ORDER BY CurrentStock ASC;
        DROP TABLE #LowStockItems;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        INSERT INTO dbo.ErrorLog (ProcedureName, ErrorMessage, ErrorLine, OccurredAt)
        VALUES ('SP_Inventory_Management', ERROR_MESSAGE(), ERROR_LINE(), GETDATE());
        THROW;
    END CATCH;
END
"""
    },

    "SP_Customer_Segmentation_ML": {
        "name": "SP_Customer_Segmentation_ML",
        "description": "Customer RFM segmentation with scoring, pivoting, and ML feature engineering",
        "code": """
CREATE PROCEDURE [dbo].[SP_Customer_Segmentation_ML]
    @AnalysisDate       DATE = NULL,
    @MinTransactions    INT  = 2,
    @OutputToTable      BIT  = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET @AnalysisDate = ISNULL(@AnalysisDate, CAST(GETDATE() AS DATE));

    -- RFM Calculation
    ;WITH RFM_Base AS (
        SELECT
            c.CustomerID,
            c.CustomerName,
            c.Email,
            c.Region,
            c.TierLevel,
            c.AcquisitionDate,
            DATEDIFF(DAY, MAX(o.OrderDate), @AnalysisDate)   AS Recency,
            COUNT(DISTINCT o.OrderID)                          AS Frequency,
            SUM(o.OrderTotal)                                  AS Monetary,
            AVG(o.OrderTotal)                                  AS AvgOrderValue,
            MIN(o.OrderDate)                                   AS FirstOrderDate,
            MAX(o.OrderDate)                                   AS LastOrderDate,
            STDEV(o.OrderTotal)                                AS OrderValueStdDev,
            SUM(CASE WHEN o.ReturnFlag = 1 THEN 1 ELSE 0 END) AS ReturnCount
        FROM dbo.Customers c
        INNER JOIN dbo.Orders o ON c.CustomerID = o.CustomerID
        WHERE o.OrderDate <= @AnalysisDate
          AND o.Status NOT IN ('CANCELLED','FRAUD')
          AND c.IsActive = 1
        GROUP BY
            c.CustomerID, c.CustomerName, c.Email,
            c.Region, c.TierLevel, c.AcquisitionDate
        HAVING COUNT(DISTINCT o.OrderID) >= @MinTransactions
    ),
    -- Percentile Scoring
    RFM_Percentiles AS (
        SELECT *,
            NTILE(5) OVER (ORDER BY Recency ASC)   AS R_Score,
            NTILE(5) OVER (ORDER BY Frequency DESC) AS F_Score,
            NTILE(5) OVER (ORDER BY Monetary DESC)  AS M_Score
        FROM RFM_Base
    ),
    -- Product Category Preferences (Pivot)
    CategorySpend AS (
        SELECT
            o.CustomerID,
            p.CategoryName,
            SUM(ol.LineTotal) AS CategoryTotal
        FROM dbo.Orders o
        JOIN dbo.OrderLines ol ON o.OrderID = ol.OrderID
        JOIN dbo.Products    p  ON ol.ProductID = p.ProductID
        WHERE o.OrderDate <= @AnalysisDate
        GROUP BY o.CustomerID, p.CategoryName
    ),
    TopCategory AS (
        SELECT
            CustomerID,
            CategoryName AS TopSpendCategory,
            CategoryTotal,
            ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY CategoryTotal DESC) AS rn
        FROM CategorySpend
    ),
    -- Final Segmentation
    Segmented AS (
        SELECT
            r.*,
            (r.R_Score + r.F_Score + r.M_Score) AS RFM_Total,
            CASE
                WHEN r.R_Score >= 4 AND r.F_Score >= 4 AND r.M_Score >= 4  THEN 'Champions'
                WHEN r.R_Score >= 3 AND r.F_Score >= 3                      THEN 'Loyal Customers'
                WHEN r.R_Score >= 4 AND r.F_Score <= 2                      THEN 'Recent Customers'
                WHEN r.R_Score >= 3 AND r.M_Score >= 4                      THEN 'Potential Loyalists'
                WHEN r.R_Score <= 2 AND r.F_Score >= 4 AND r.M_Score >= 4   THEN 'At Risk'
                WHEN r.R_Score <= 2 AND r.F_Score >= 3                      THEN 'Cant Lose Them'
                WHEN r.R_Score <= 2 AND r.F_Score <= 2 AND r.M_Score <= 2   THEN 'Lost'
                ELSE 'Needs Attention'
            END AS Segment,
            tc.TopSpendCategory,
            DATEDIFF(DAY, r.AcquisitionDate, @AnalysisDate)  AS CustomerAgeDays,
            ROUND(r.ReturnCount * 1.0 / NULLIF(r.Frequency, 0), 4) AS ReturnRate
        FROM RFM_Percentiles r
        LEFT JOIN TopCategory tc ON r.CustomerID = tc.CustomerID AND tc.rn = 1
    )
    SELECT * INTO #CustomerSegments FROM Segmented;

    IF @OutputToTable = 1
    BEGIN
        MERGE dbo.CustomerSegmentation AS target
        USING #CustomerSegments AS src
        ON target.CustomerID = src.CustomerID
        WHEN MATCHED THEN
            UPDATE SET
                Segment          = src.Segment,
                RFM_Total        = src.RFM_Total,
                R_Score          = src.R_Score,
                F_Score          = src.F_Score,
                M_Score          = src.M_Score,
                LastUpdated      = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (CustomerID, Segment, RFM_Total, R_Score, F_Score, M_Score, CreatedAt, LastUpdated)
            VALUES (src.CustomerID, src.Segment, src.RFM_Total,
                    src.R_Score, src.F_Score, src.M_Score, GETDATE(), GETDATE());
    END;

    SELECT * FROM #CustomerSegments ORDER BY RFM_Total DESC, Monetary DESC;
    DROP TABLE #CustomerSegments;
END
"""
    },

    "SP_Financial_Reporting_GL": {
        "name": "SP_Financial_Reporting_GL",
        "description": "General ledger financial reporting with trial balance, P&L, and balance sheet",
        "code": """
CREATE PROCEDURE [dbo].[SP_Financial_Reporting_GL]
    @FiscalYear     INT,
    @FiscalPeriod   INT = NULL,
    @EntityID       INT = NULL,
    @CurrencyCode   NVARCHAR(3) = 'USD'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ExchangeRate DECIMAL(18,6);
    SELECT @ExchangeRate = ISNULL(ExchangeRate, 1)
    FROM dbo.CurrencyRates
    WHERE CurrencyCode = @CurrencyCode
      AND RateDate = (SELECT MAX(RateDate) FROM dbo.CurrencyRates WHERE CurrencyCode = @CurrencyCode);

    -- Trial Balance
    ;WITH JournalEntries AS (
        SELECT
            je.AccountID,
            a.AccountCode,
            a.AccountName,
            a.AccountType,
            a.ParentAccountID,
            je.DebitAmount  * @ExchangeRate AS DebitAmountUSD,
            je.CreditAmount * @ExchangeRate AS CreditAmountUSD,
            je.FiscalYear,
            je.FiscalPeriod,
            je.EntityID,
            je.CostCenterID,
            je.ProjectID
        FROM dbo.JournalEntries je
        INNER JOIN dbo.ChartOfAccounts a ON je.AccountID = a.AccountID
        WHERE je.FiscalYear = @FiscalYear
          AND je.IsPosted = 1
          AND je.IsReversed = 0
          AND (@FiscalPeriod IS NULL OR je.FiscalPeriod <= @FiscalPeriod)
          AND (@EntityID IS NULL OR je.EntityID = @EntityID)
    ),
    TrialBalance AS (
        SELECT
            AccountID,
            AccountCode,
            AccountName,
            AccountType,
            ParentAccountID,
            SUM(DebitAmountUSD)   AS TotalDebits,
            SUM(CreditAmountUSD)  AS TotalCredits,
            SUM(DebitAmountUSD) - SUM(CreditAmountUSD) AS NetBalance
        FROM JournalEntries
        GROUP BY AccountID, AccountCode, AccountName, AccountType, ParentAccountID
    ),
    -- Account Hierarchy (recursive)
    AccountHierarchy AS (
        SELECT
            AccountID,
            AccountCode,
            AccountName,
            AccountType,
            ParentAccountID,
            AccountCode AS HierarchyPath,
            0           AS Level
        FROM dbo.ChartOfAccounts
        WHERE ParentAccountID IS NULL

        UNION ALL

        SELECT
            c.AccountID,
            c.AccountCode,
            c.AccountName,
            c.AccountType,
            c.ParentAccountID,
            CAST(h.HierarchyPath + ' > ' + c.AccountCode AS NVARCHAR(500)),
            h.Level + 1
        FROM dbo.ChartOfAccounts c
        INNER JOIN AccountHierarchy h ON c.ParentAccountID = h.AccountID
    ),
    -- P&L Components
    PnL AS (
        SELECT
            tb.AccountID,
            tb.AccountCode,
            tb.AccountName,
            tb.AccountType,
            ah.HierarchyPath,
            ah.Level,
            tb.TotalDebits,
            tb.TotalCredits,
            CASE
                WHEN tb.AccountType IN ('Revenue','Other Income')   THEN tb.TotalCredits - tb.TotalDebits
                WHEN tb.AccountType IN ('COGS','Operating Expense') THEN tb.TotalDebits  - tb.TotalCredits
                ELSE tb.NetBalance
            END AS ReportingBalance,
            SUM(
                CASE
                    WHEN tb.AccountType IN ('Revenue','Other Income')   THEN tb.TotalCredits - tb.TotalDebits
                    WHEN tb.AccountType IN ('COGS','Operating Expense') THEN tb.TotalDebits  - tb.TotalCredits
                    ELSE 0
                END
            ) OVER (PARTITION BY tb.AccountType) AS SubtotalByType,
            ROW_NUMBER() OVER (
                PARTITION BY tb.AccountType
                ORDER BY tb.AccountCode
            ) AS LineOrder
        FROM TrialBalance tb
        LEFT JOIN AccountHierarchy ah ON tb.AccountID = ah.AccountID
    )
    SELECT
        p.*,
        @FiscalYear    AS FiscalYear,
        @FiscalPeriod  AS FiscalPeriod,
        @CurrencyCode  AS ReportCurrency,
        GETDATE()      AS GeneratedAt
    FROM PnL p
    ORDER BY AccountType, AccountCode;
END
"""
    },

    "SP_ETL_DataPipeline_Staging": {
        "name": "SP_ETL_DataPipeline_Staging",
        "description": "Full ETL pipeline: extract, validate, transform, load with SCD Type 2 and audit",
        "code": """
CREATE PROCEDURE [dbo].[SP_ETL_DataPipeline_Staging]
    @BatchID        UNIQUEIDENTIFIER = NULL,
    @SourceSystem   NVARCHAR(50),
    @TargetTable    NVARCHAR(128),
    @LoadMode       NVARCHAR(20) = 'INCREMENTAL',   -- FULL | INCREMENTAL | CDC
    @WatermarkDate  DATETIME    = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @BatchID = ISNULL(@BatchID, NEWID());

    DECLARE @StartTime   DATETIME = GETDATE(),
            @RowsRead    INT = 0,
            @RowsInserted INT = 0,
            @RowsUpdated  INT = 0,
            @RowsRejected INT = 0,
            @ErrorMsg     NVARCHAR(MAX);

    -- Batch Log: Start
    INSERT INTO dbo.ETL_BatchLog (BatchID, SourceSystem, TargetTable, LoadMode, StartTime, Status)
    VALUES (@BatchID, @SourceSystem, @TargetTable, @LoadMode, @StartTime, 'RUNNING');

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Stage 1: Extract to Landing
        CREATE TABLE #Landing (
            RowID           INT IDENTITY(1,1),
            RawData         NVARCHAR(MAX),
            SourceKey       NVARCHAR(255),
            ExtractedAt     DATETIME DEFAULT GETDATE(),
            ValidationStatus NVARCHAR(20) DEFAULT 'PENDING',
            ValidationMsg   NVARCHAR(500)
        );

        INSERT INTO #Landing (RawData, SourceKey)
        SELECT
            (SELECT * FROM dbo.SourceData sd WHERE sd.SourceSystem = @SourceSystem
             AND (@LoadMode = 'FULL' OR sd.ModifiedAt >= ISNULL(@WatermarkDate, '1900-01-01'))
             FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            sd.NaturalKey
        FROM dbo.SourceData sd
        WHERE sd.SourceSystem = @SourceSystem
          AND (@LoadMode = 'FULL' OR sd.ModifiedAt >= ISNULL(@WatermarkDate, '1900-01-01'));

        SET @RowsRead = @@ROWCOUNT;

        -- Stage 2: Data Quality Validation
        UPDATE l
        SET ValidationStatus = 'FAILED',
            ValidationMsg    = CASE
                WHEN l.SourceKey IS NULL OR LEN(TRIM(l.SourceKey)) = 0
                    THEN 'Missing source key'
                WHEN LEN(l.RawData) > 65000
                    THEN 'Record exceeds size limit'
                WHEN EXISTS (
                    SELECT 1 FROM dbo.MandatoryFieldRules mf
                    WHERE mf.SourceSystem = @SourceSystem
                      AND JSON_VALUE(l.RawData, mf.JsonPath) IS NULL
                ) THEN 'Missing mandatory field'
                ELSE 'UNKNOWN'
            END
        FROM #Landing l
        WHERE l.SourceKey IS NULL
           OR LEN(TRIM(ISNULL(l.SourceKey,''))) = 0
           OR LEN(l.RawData) > 65000;

        UPDATE #Landing SET ValidationStatus = 'PASSED'
        WHERE ValidationStatus = 'PENDING';

        SELECT @RowsRejected = COUNT(*) FROM #Landing WHERE ValidationStatus = 'FAILED';

        -- Stage 3: SCD Type 2 Merge into Target
        DECLARE @SQL NVARCHAR(MAX) = N'
        MERGE dbo.' + QUOTENAME(@TargetTable) + N' AS tgt
        USING (
            SELECT
                l.SourceKey,
                JSON_VALUE(l.RawData, ''$.BusinessKey'')    AS BusinessKey,
                JSON_VALUE(l.RawData, ''$.Attributes'')     AS Attributes,
                CAST(JSON_VALUE(l.RawData, ''$.RecordHash'') AS NVARCHAR(64)) AS RecordHash,
                l.ExtractedAt
            FROM #Landing l
            WHERE l.ValidationStatus = ''PASSED''
        ) AS src
        ON tgt.BusinessKey = src.BusinessKey AND tgt.IsCurrent = 1
        WHEN MATCHED AND tgt.RecordHash <> src.RecordHash THEN
            UPDATE SET
                tgt.IsCurrent    = 0,
                tgt.EffectiveEnd = src.ExtractedAt
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SourceKey, BusinessKey, Attributes, RecordHash, IsCurrent, EffectiveStart, EffectiveEnd, BatchID)
            VALUES (src.SourceKey, src.BusinessKey, src.Attributes, src.RecordHash,
                    1, src.ExtractedAt, ''9999-12-31'', ''' + CAST(@BatchID AS NVARCHAR(36)) + N''');';

        EXEC sp_executesql @SQL;

        SET @RowsUpdated  = @@ROWCOUNT;

        -- Insert NEW records for updated ones (SCD2 versioning)
        INSERT INTO dbo.ETL_RejectedRecords (BatchID, SourceSystem, SourceKey, RawData, ValidationMsg, RejectedAt)
        SELECT @BatchID, @SourceSystem, SourceKey, RawData, ValidationMsg, GETDATE()
        FROM #Landing WHERE ValidationStatus = 'FAILED';

        -- Update Watermark
        MERGE dbo.ETL_Watermarks AS w
        USING (SELECT @SourceSystem AS SourceSystem, MAX(ExtractedAt) AS NewWatermark FROM #Landing) AS s
        ON w.SourceSystem = s.SourceSystem
        WHEN MATCHED THEN UPDATE SET LastLoadTime = s.NewWatermark
        WHEN NOT MATCHED THEN INSERT (SourceSystem, LastLoadTime) VALUES (s.SourceSystem, s.NewWatermark);

        COMMIT TRANSACTION;

        -- Update Batch Log: Success
        UPDATE dbo.ETL_BatchLog
        SET Status        = 'SUCCEEDED',
            EndTime       = GETDATE(),
            RowsRead      = @RowsRead,
            RowsInserted  = @RowsInserted,
            RowsUpdated   = @RowsUpdated,
            RowsRejected  = @RowsRejected,
            DurationSeconds = DATEDIFF(SECOND, @StartTime, GETDATE())
        WHERE BatchID = @BatchID;

        DROP TABLE #Landing;

        SELECT
            @BatchID          AS BatchID,
            'SUCCEEDED'       AS Status,
            @RowsRead         AS RowsRead,
            @RowsInserted     AS RowsInserted,
            @RowsUpdated      AS RowsUpdated,
            @RowsRejected     AS RowsRejected,
            DATEDIFF(SECOND, @StartTime, GETDATE()) AS DurationSeconds;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @ErrorMsg = ERROR_MESSAGE();

        UPDATE dbo.ETL_BatchLog
        SET Status = 'FAILED', EndTime = GETDATE(), ErrorMessage = @ErrorMsg
        WHERE BatchID = @BatchID;

        INSERT INTO dbo.ErrorLog (ProcedureName, ErrorMessage, ErrorLine, OccurredAt)
        VALUES ('SP_ETL_DataPipeline_Staging', @ErrorMsg, ERROR_LINE(), GETDATE());

        IF OBJECT_ID('tempdb..#Landing') IS NOT NULL DROP TABLE #Landing;
        THROW;
    END CATCH;
END
"""
    }
}

# ═════════════════════════════════════════════════════════════════════════════
# SQL VIEWS
# ═════════════════════════════════════════════════════════════════════════════
SQL_VIEWS = {
    "VW_CustomerOrderSummary": {
        "name"       : "VW_CustomerOrderSummary",
        "object_type": "view",
        "description": "Customer order summary with lifetime value, frequency, and recency metrics",
        "code": """
CREATE OR ALTER VIEW [dbo].[VW_CustomerOrderSummary]
AS
    WITH OrderStats AS (
        SELECT
            o.CustomerID,
            COUNT(DISTINCT o.OrderID)                                AS TotalOrders,
            SUM(o.OrderTotal)                                        AS LifetimeValue,
            AVG(o.OrderTotal)                                        AS AvgOrderValue,
            MIN(o.OrderDate)                                         AS FirstOrderDate,
            MAX(o.OrderDate)                                         AS LastOrderDate,
            DATEDIFF(DAY, MAX(o.OrderDate), CAST(GETDATE() AS DATE)) AS DaysSinceLastOrder,
            SUM(CASE WHEN o.ReturnFlag = 1 THEN 1 ELSE 0 END)       AS TotalReturns,
            COUNT(DISTINCT YEAR(o.OrderDate))                        AS ActiveYears
        FROM dbo.Orders o
        WHERE o.Status NOT IN ('CANCELLED', 'FRAUD')
        GROUP BY o.CustomerID
    ),
    CustomerTier AS (
        SELECT
            CustomerID,
            CASE
                WHEN LifetimeValue >= 50000  THEN 'Platinum'
                WHEN LifetimeValue >= 10000  THEN 'Gold'
                WHEN LifetimeValue >= 2500   THEN 'Silver'
                ELSE 'Bronze'
            END AS ComputedTier,
            RANK() OVER (ORDER BY LifetimeValue DESC) AS ValueRank
        FROM OrderStats
    )
    SELECT
        c.CustomerID,
        c.CustomerName,
        c.Email,
        c.Region,
        c.TierLevel,
        os.TotalOrders,
        os.LifetimeValue,
        os.AvgOrderValue,
        os.FirstOrderDate,
        os.LastOrderDate,
        os.DaysSinceLastOrder,
        os.TotalReturns,
        os.ActiveYears,
        ct.ComputedTier,
        ct.ValueRank,
        ROUND(CAST(os.TotalReturns AS FLOAT) / NULLIF(os.TotalOrders, 0) * 100, 2) AS ReturnRatePct,
        CASE
            WHEN os.DaysSinceLastOrder <= 30   THEN 'Active'
            WHEN os.DaysSinceLastOrder <= 90   THEN 'At Risk'
            WHEN os.DaysSinceLastOrder <= 180  THEN 'Lapsing'
            ELSE 'Churned'
        END AS EngagementStatus
    FROM dbo.Customers c
    INNER JOIN OrderStats  os ON c.CustomerID = os.CustomerID
    INNER JOIN CustomerTier ct ON c.CustomerID = ct.CustomerID
    WHERE c.IsActive = 1;
"""
    },

    "VW_ProductInventoryStatus": {
        "name"       : "VW_ProductInventoryStatus",
        "object_type": "view",
        "description": "Product inventory status with reorder alerts, ABC classification, and supplier info",
        "code": """
CREATE OR ALTER VIEW [dbo].[VW_ProductInventoryStatus]
AS
    WITH InventoryABC AS (
        SELECT
            p.ProductID,
            SUM(ol.Quantity * ol.UnitPrice) AS AnnualRevenue,
            NTILE(3) OVER (ORDER BY SUM(ol.Quantity * ol.UnitPrice) DESC) AS ABCRank
        FROM dbo.Products p
        JOIN dbo.OrderLines ol ON p.ProductID = ol.ProductID
        JOIN dbo.Orders     o  ON ol.OrderID   = o.OrderID
        WHERE o.OrderDate >= DATEADD(YEAR, -1, GETDATE())
          AND o.Status NOT IN ('CANCELLED', 'FRAUD')
        GROUP BY p.ProductID
    ),
    RecentSales AS (
        SELECT
            ol.ProductID,
            AVG(CAST(ol.Quantity AS FLOAT))        AS AvgDailySales,
            SUM(ol.Quantity)                        AS TotalUnitsSold90d,
            COUNT(DISTINCT o.OrderID)               AS OrderCount90d
        FROM dbo.OrderLines ol
        JOIN dbo.Orders o ON ol.OrderID = o.OrderID
        WHERE o.OrderDate >= DATEADD(DAY, -90, GETDATE())
          AND o.Status NOT IN ('CANCELLED', 'FRAUD')
        GROUP BY ol.ProductID
    )
    SELECT
        p.ProductID,
        p.ProductName,
        p.CategoryID,
        cat.CategoryName,
        p.SKU,
        p.ReorderPoint,
        p.ReorderQuantity,
        p.LeadTimeDays,
        i.WarehouseID,
        w.WarehouseName,
        i.QuantityOnHand,
        i.QuantityReserved,
        i.QuantityOnHand - ISNULL(i.QuantityReserved, 0) AS AvailableStock,
        ps.SupplierID,
        s.SupplierName,
        ps.UnitCost,
        rs.AvgDailySales,
        rs.TotalUnitsSold90d,
        CASE
            WHEN abc.ABCRank = 1 THEN 'A'
            WHEN abc.ABCRank = 2 THEN 'B'
            ELSE 'C'
        END AS ABCClass,
        CASE
            WHEN i.QuantityOnHand <= 0             THEN 'CRITICAL - OUT OF STOCK'
            WHEN i.QuantityOnHand <= p.ReorderPoint * 0.5 THEN 'URGENT - REORDER NOW'
            WHEN i.QuantityOnHand <= p.ReorderPoint        THEN 'LOW - REORDER SOON'
            WHEN i.QuantityOnHand >= p.ReorderPoint * 3    THEN 'EXCESS STOCK'
            ELSE 'OPTIMAL'
        END AS StockStatus,
        CASE
            WHEN ISNULL(rs.AvgDailySales, 0) > 0
            THEN CAST(ROUND(i.QuantityOnHand / rs.AvgDailySales, 0) AS INT)
            ELSE NULL
        END AS DaysOfStockRemaining,
        p.IsDiscontinued,
        p.LastModified
    FROM dbo.Products p
    JOIN dbo.Inventory      i   ON p.ProductID    = i.ProductID
    JOIN dbo.Warehouses     w   ON i.WarehouseID  = w.WarehouseID
    JOIN dbo.Categories     cat ON p.CategoryID   = cat.CategoryID
    LEFT JOIN dbo.ProductSupplier ps ON p.ProductID = ps.ProductID AND ps.IsPrimary = 1
    LEFT JOIN dbo.Suppliers       s  ON ps.SupplierID = s.SupplierID
    LEFT JOIN InventoryABC        abc ON p.ProductID = abc.ProductID
    LEFT JOIN RecentSales         rs  ON p.ProductID = rs.ProductID
    WHERE p.IsDiscontinued = 0;
"""
    },

    "VW_FinancialPeriodSummary": {
        "name"       : "VW_FinancialPeriodSummary",
        "object_type": "view",
        "description": "Financial period P&L summary with YoY comparisons and variance analysis",
        "code": """
CREATE OR ALTER VIEW [dbo].[VW_FinancialPeriodSummary]
AS
    WITH CurrentPeriod AS (
        SELECT
            je.EntityID,
            e.EntityName,
            je.FiscalYear,
            je.FiscalPeriod,
            a.AccountType,
            SUM(CASE WHEN a.AccountType IN ('Revenue','Other Income')
                     THEN je.CreditAmount - je.DebitAmount
                     ELSE je.DebitAmount  - je.CreditAmount END) AS NetAmount,
            COUNT(DISTINCT je.JournalID)     AS JournalCount,
            COUNT(DISTINCT je.CostCenterID)  AS CostCenterCount
        FROM dbo.JournalEntries je
        JOIN dbo.ChartOfAccounts a ON je.AccountID  = a.AccountID
        JOIN dbo.Entities        e ON je.EntityID   = e.EntityID
        WHERE je.IsPosted   = 1
          AND je.IsReversed = 0
        GROUP BY je.EntityID, e.EntityName, je.FiscalYear, je.FiscalPeriod, a.AccountType
    ),
    PivotedPL AS (
        SELECT
            EntityID,
            EntityName,
            FiscalYear,
            FiscalPeriod,
            SUM(CASE WHEN AccountType IN ('Revenue','Other Income')   THEN NetAmount ELSE 0 END) AS TotalRevenue,
            SUM(CASE WHEN AccountType = 'COGS'                        THEN NetAmount ELSE 0 END) AS TotalCOGS,
            SUM(CASE WHEN AccountType = 'Operating Expense'           THEN NetAmount ELSE 0 END) AS TotalOpEx,
            SUM(CASE WHEN AccountType = 'Other Expense'               THEN NetAmount ELSE 0 END) AS OtherExpenses,
            SUM(JournalCount)                                                                      AS TotalJournals
        FROM CurrentPeriod
        GROUP BY EntityID, EntityName, FiscalYear, FiscalPeriod
    )
    SELECT
        p.EntityID,
        p.EntityName,
        p.FiscalYear,
        p.FiscalPeriod,
        p.TotalRevenue,
        p.TotalCOGS,
        p.TotalRevenue - p.TotalCOGS                                              AS GrossProfit,
        CASE WHEN p.TotalRevenue <> 0
             THEN ROUND((p.TotalRevenue - p.TotalCOGS) / p.TotalRevenue * 100, 2)
             ELSE NULL END                                                         AS GrossMarginPct,
        p.TotalOpEx,
        p.TotalRevenue - p.TotalCOGS - p.TotalOpEx                               AS EBITDA,
        CASE WHEN p.TotalRevenue <> 0
             THEN ROUND((p.TotalRevenue - p.TotalCOGS - p.TotalOpEx) / p.TotalRevenue * 100, 2)
             ELSE NULL END                                                         AS EBITDAMarginPct,
        p.OtherExpenses,
        p.TotalRevenue - p.TotalCOGS - p.TotalOpEx - p.OtherExpenses             AS NetIncome,
        p.TotalJournals,
        -- Prior year same period for comparison
        prev.TotalRevenue   AS PY_Revenue,
        prev.EBITDA         AS PY_EBITDA,
        CASE WHEN ISNULL(prev.TotalRevenue,0) <> 0
             THEN ROUND((p.TotalRevenue - prev.TotalRevenue) / ABS(prev.TotalRevenue) * 100, 2)
             ELSE NULL END  AS YoY_RevenuePct
    FROM PivotedPL p
    LEFT JOIN (
        SELECT EntityID, FiscalYear, FiscalPeriod, TotalRevenue,
               TotalRevenue - TotalCOGS - TotalOpEx AS EBITDA
        FROM PivotedPL
    ) prev ON p.EntityID     = prev.EntityID
           AND p.FiscalYear   = prev.FiscalYear + 1
           AND p.FiscalPeriod = prev.FiscalPeriod;
"""
    }
}

# ═════════════════════════════════════════════════════════════════════════════
# SQL USER-DEFINED FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════
SQL_UDFS = {
    "UDF_CalculateTax": {
        "name"       : "UDF_CalculateTax",
        "object_type": "udf",
        "description": "Calculates tax amount based on region, product category, and order value with tiered rates",
        "code": """
CREATE OR ALTER FUNCTION [dbo].[UDF_CalculateTax]
(
    @OrderValue     DECIMAL(18,4),
    @RegionCode     NVARCHAR(10),
    @CategoryCode   NVARCHAR(20),
    @CustomerType   NVARCHAR(20)   -- 'B2B' | 'B2C' | 'EXEMPT'
)
RETURNS DECIMAL(18,4)
AS
BEGIN
    DECLARE @TaxRate     DECIMAL(7,4) = 0.0;
    DECLARE @TaxAmount   DECIMAL(18,4);
    DECLARE @StateTax    DECIMAL(7,4) = 0.0;
    DECLARE @FederalTax  DECIMAL(7,4) = 0.0;
    DECLARE @SpecialRate DECIMAL(7,4) = 0.0;

    -- Exempt customers pay no tax
    IF @CustomerType = 'EXEMPT'
    BEGIN
        RETURN 0.0;
    END;

    -- State/Region base tax rate
    SET @StateTax = CASE @RegionCode
        WHEN 'CA' THEN 0.0725  WHEN 'NY' THEN 0.0800
        WHEN 'TX' THEN 0.0625  WHEN 'FL' THEN 0.0600
        WHEN 'WA' THEN 0.1025  WHEN 'IL' THEN 0.0875
        WHEN 'PA' THEN 0.0600  WHEN 'OH' THEN 0.0575
        ELSE 0.0500
    END;

    -- Federal surcharge for high-value orders
    SET @FederalTax = CASE
        WHEN @OrderValue >= 100000 THEN 0.0050
        WHEN @OrderValue >= 50000  THEN 0.0025
        ELSE 0.0
    END;

    -- Category-based special rates (luxury/digital/food)
    SET @SpecialRate = CASE @CategoryCode
        WHEN 'LUXURY'    THEN 0.0200
        WHEN 'DIGITAL'   THEN 0.0100
        WHEN 'FOOD'      THEN -0.0200   -- food tax reduction
        WHEN 'MEDICINE'  THEN -0.0500   -- medicine tax reduction
        ELSE 0.0
    END;

    -- B2B gets 50% tax credit on state tax
    IF @CustomerType = 'B2B'
        SET @StateTax = @StateTax * 0.50;

    -- Aggregate and floor at 0
    SET @TaxRate = @StateTax + @FederalTax + @SpecialRate;
    IF @TaxRate < 0 SET @TaxRate = 0.0;

    SET @TaxAmount = ROUND(@OrderValue * @TaxRate, 4);
    RETURN @TaxAmount;
END;
"""
    },

    "UDF_FormatCurrency": {
        "name"       : "UDF_FormatCurrency",
        "object_type": "udf",
        "description": "Formats a numeric value as a localized currency string with symbol and thousand separators",
        "code": """
CREATE OR ALTER FUNCTION [dbo].[UDF_FormatCurrency]
(
    @Amount         DECIMAL(18,4),
    @CurrencyCode   NVARCHAR(3)  = 'USD',
    @DecimalPlaces  INT          = 2,
    @ShowSymbol     BIT          = 1
)
RETURNS NVARCHAR(50)
AS
BEGIN
    DECLARE @Symbol          NVARCHAR(5);
    DECLARE @FormattedNumber NVARCHAR(40);
    DECLARE @Negative        BIT = 0;
    DECLARE @AbsAmount       DECIMAL(18,4) = ABS(@Amount);
    DECLARE @IntPart         BIGINT;
    DECLARE @DecPart         NVARCHAR(10);
    DECLARE @IntStr          NVARCHAR(30);
    DECLARE @Result          NVARCHAR(50);
    DECLARE @GroupedInt      NVARCHAR(30) = '';
    DECLARE @Pos             INT;

    IF @Amount < 0 SET @Negative = 1;

    -- Currency symbol mapping
    SET @Symbol = CASE @CurrencyCode
        WHEN 'USD' THEN '$'   WHEN 'EUR' THEN '€'
        WHEN 'GBP' THEN '£'   WHEN 'JPY' THEN '¥'
        WHEN 'INR' THEN '₹'   WHEN 'CAD' THEN 'C$'
        WHEN 'AUD' THEN 'A$'  WHEN 'CHF' THEN 'Fr'
        ELSE @CurrencyCode + ' '
    END;

    -- Split integer and decimal parts
    SET @IntPart = FLOOR(@AbsAmount);
    SET @DecPart = RIGHT('00' + CAST(
        ROUND((@AbsAmount - @IntPart) * POWER(10, @DecimalPlaces), 0) AS NVARCHAR(10)), @DecimalPlaces);

    -- Thousand separator grouping
    SET @IntStr = CAST(@IntPart AS NVARCHAR(30));
    SET @Pos    = LEN(@IntStr);
    WHILE @Pos > 3
    BEGIN
        SET @GroupedInt = ',' + SUBSTRING(@IntStr, @Pos - 2, 3) + @GroupedInt;
        SET @IntStr = LEFT(@IntStr, @Pos - 3);
        SET @Pos = @Pos - 3;
    END;
    SET @GroupedInt = @IntStr + @GroupedInt;

    -- Assemble result
    IF @DecimalPlaces > 0
        SET @FormattedNumber = @GroupedInt + '.' + @DecPart;
    ELSE
        SET @FormattedNumber = @GroupedInt;

    IF @ShowSymbol = 1
        SET @Result = @Symbol + @FormattedNumber;
    ELSE
        SET @Result = @FormattedNumber + ' ' + @CurrencyCode;

    IF @Negative = 1
        SET @Result = '(' + @Result + ')';

    RETURN @Result;
END;
"""
    },

    "UDF_GetFiscalPeriod": {
        "name"       : "UDF_GetFiscalPeriod",
        "object_type": "udf",
        "description": "Derives fiscal year, period, quarter, and week number from a calendar date with configurable fiscal year start month",
        "code": """
CREATE OR ALTER FUNCTION [dbo].[UDF_GetFiscalPeriod]
(
    @InputDate          DATE,
    @FiscalYearStartMon INT = 4,    -- Default: April (Indian/UK fiscal year)
    @ReturnPart         NVARCHAR(20) = 'YEAR'
        -- Options: YEAR | PERIOD | QUARTER | WEEK | HALFYEAR
        --          YEARPERIOD (e.g., 2024-03) | LABEL (e.g., FY2024-Q3)
)
RETURNS NVARCHAR(20)
AS
BEGIN
    DECLARE @CalYear   INT = YEAR(@InputDate);
    DECLARE @CalMonth  INT = MONTH(@InputDate);
    DECLARE @CalDay    INT = DAY(@InputDate);

    DECLARE @FiscalYear    INT;
    DECLARE @FiscalPeriod  INT;   -- 1-12, where 1 = @FiscalYearStartMon
    DECLARE @FiscalQuarter INT;
    DECLARE @FiscalHalf    INT;
    DECLARE @FiscalWeek    INT;

    -- Derive Fiscal Period (1-indexed relative to start month)
    SET @FiscalPeriod = ((@CalMonth - @FiscalYearStartMon + 12) % 12) + 1;

    -- Derive Fiscal Year
    IF @CalMonth >= @FiscalYearStartMon
        SET @FiscalYear = @CalYear;
    ELSE
        SET @FiscalYear = @CalYear - 1;

    -- Derive Fiscal Quarter
    SET @FiscalQuarter = CEILING(CAST(@FiscalPeriod AS FLOAT) / 3);

    -- Derive Fiscal Half-Year
    SET @FiscalHalf = CASE WHEN @FiscalPeriod <= 6 THEN 1 ELSE 2 END;

    -- Derive Fiscal Week (ISO-style, offset from fiscal year start)
    DECLARE @FYStart DATE = DATEFROMPARTS(
        CASE WHEN @CalMonth >= @FiscalYearStartMon THEN @CalYear ELSE @CalYear - 1 END,
        @FiscalYearStartMon, 1
    );
    SET @FiscalWeek = DATEDIFF(WEEK, @FYStart, @InputDate) + 1;
    IF @FiscalWeek > 52 SET @FiscalWeek = 52;

    -- Return requested part
    RETURN CASE @ReturnPart
        WHEN 'YEAR'       THEN CAST(@FiscalYear AS NVARCHAR)
        WHEN 'PERIOD'     THEN CAST(@FiscalPeriod AS NVARCHAR)
        WHEN 'QUARTER'    THEN CAST(@FiscalQuarter AS NVARCHAR)
        WHEN 'HALFYEAR'   THEN CAST(@FiscalHalf AS NVARCHAR)
        WHEN 'WEEK'       THEN CAST(@FiscalWeek AS NVARCHAR)
        WHEN 'YEARPERIOD' THEN CAST(@FiscalYear AS NVARCHAR) + '-' +
                               RIGHT('0' + CAST(@FiscalPeriod AS NVARCHAR), 2)
        WHEN 'LABEL'      THEN 'FY' + CAST(@FiscalYear AS NVARCHAR) + '-Q' +
                               CAST(@FiscalQuarter AS NVARCHAR)
        ELSE CAST(@FiscalYear AS NVARCHAR)
    END;
END;
"""
    }
}

# ═════════════════════════════════════════════════════════════════════════════
# SQL TABLES — demo table DDLs for Discovery analysis
# ═════════════════════════════════════════════════════════════════════════════
SQL_TABLES = {
    "Customers": {
        "name": "Customers",
        "object_type": "table",
        "description": "Core customer master table — clean, standard types",
        "column_count": 8,
        "row_count": 125000,
        "has_triggers": False,
        "index_count": 3,
        "fk_count": 0,
        "check_count": 1,
        "code": """
CREATE TABLE [dbo].[Customers] (
  [CustomerID]   INT            IDENTITY(1,1) NOT NULL,
  [CustomerName] NVARCHAR(200)  NOT NULL,
  [Email]        NVARCHAR(255)  NOT NULL,
  [Phone]        NVARCHAR(20)   NULL,
  [Region]       NVARCHAR(50)   NOT NULL DEFAULT 'US',
  [CreatedDate]  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  [IsActive]     BIT            NOT NULL DEFAULT 1,
  [CreditLimit]  DECIMAL(18,2)  NULL,
  CONSTRAINT PK_Customers PRIMARY KEY CLUSTERED (CustomerID),
  CONSTRAINT UQ_Customers_Email UNIQUE (Email),
  CONSTRAINT CK_Customers_CreditLimit CHECK (CreditLimit >= 0)
);
"""
    },
    "Orders": {
        "name": "Orders",
        "object_type": "table",
        "description": "Order header table with foreign keys and triggers",
        "column_count": 12,
        "row_count": 4500000,
        "has_triggers": True,
        "index_count": 5,
        "fk_count": 2,
        "check_count": 2,
        "code": """
CREATE TABLE [dbo].[Orders] (
  [OrderID]      INT            IDENTITY(1,1) NOT NULL,
  [CustomerID]   INT            NOT NULL,
  [OrderDate]    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  [ShipDate]     DATETIME2      NULL,
  [Status]       NVARCHAR(20)   NOT NULL DEFAULT 'PENDING',
  [OrderTotal]   DECIMAL(18,2)  NOT NULL,
  [TaxAmount]    DECIMAL(18,2)  NOT NULL DEFAULT 0,
  [DiscountPct]  DECIMAL(5,2)   NULL,
  [SalesRepID]   INT            NULL,
  [WarehouseID]  INT            NOT NULL,
  [ReturnFlag]   BIT            NOT NULL DEFAULT 0,
  [Notes]        NVARCHAR(MAX)  NULL,
  CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED (OrderID),
  CONSTRAINT FK_Orders_Customer FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
  CONSTRAINT FK_Orders_SalesRep FOREIGN KEY (SalesRepID) REFERENCES Employees(EmployeeID),
  CONSTRAINT CK_Orders_Total CHECK (OrderTotal >= 0),
  CONSTRAINT CK_Orders_Status CHECK (Status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED','FRAUD'))
);
CREATE TRIGGER trg_Orders_Audit ON [dbo].[Orders] AFTER INSERT, UPDATE AS
BEGIN
  INSERT INTO AuditLog (TableName, Action, RecordID, ChangedDate)
  SELECT 'Orders', CASE WHEN EXISTS(SELECT 1 FROM deleted) THEN 'UPDATE' ELSE 'INSERT' END,
         i.OrderID, GETDATE()
  FROM inserted i;
END;
"""
    },
    "OrderDetails": {
        "name": "OrderDetails",
        "object_type": "table",
        "description": "Order line items — high volume, partitioned",
        "column_count": 9,
        "row_count": 28000000,
        "has_triggers": False,
        "index_count": 4,
        "fk_count": 2,
        "check_count": 1,
        "code": """
CREATE TABLE [dbo].[OrderDetails] (
  [DetailID]     BIGINT         IDENTITY(1,1) NOT NULL,
  [OrderID]      INT            NOT NULL,
  [ProductID]    INT            NOT NULL,
  [Quantity]     INT            NOT NULL,
  [UnitPrice]    DECIMAL(18,2)  NOT NULL,
  [LineTotal]    AS (Quantity * UnitPrice) PERSISTED,
  [Discount]     DECIMAL(5,2)   NULL DEFAULT 0,
  [ShipStatus]   NVARCHAR(20)   NOT NULL DEFAULT 'PENDING',
  [CreatedDate]  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_OrderDetails PRIMARY KEY CLUSTERED (DetailID),
  CONSTRAINT FK_OD_Order FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
  CONSTRAINT FK_OD_Product FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
  CONSTRAINT CK_OD_Qty CHECK (Quantity > 0)
) ON PartitionScheme_Monthly(CreatedDate);
"""
    },
    "Products": {
        "name": "Products",
        "object_type": "table",
        "description": "Product catalog — clean schema, standard types",
        "column_count": 10,
        "row_count": 8500,
        "has_triggers": False,
        "index_count": 3,
        "fk_count": 1,
        "check_count": 2,
        "code": """
CREATE TABLE [dbo].[Products] (
  [ProductID]    INT            IDENTITY(1,1) NOT NULL,
  [ProductName]  NVARCHAR(200)  NOT NULL,
  [SKU]          NVARCHAR(50)   NOT NULL,
  [CategoryID]   INT            NOT NULL,
  [UnitPrice]    DECIMAL(18,2)  NOT NULL,
  [StockQty]     INT            NOT NULL DEFAULT 0,
  [Weight]       DECIMAL(10,3)  NULL,
  [IsActive]     BIT            NOT NULL DEFAULT 1,
  [LaunchDate]   DATE           NULL,
  [Description]  NVARCHAR(MAX)  NULL,
  CONSTRAINT PK_Products PRIMARY KEY CLUSTERED (ProductID),
  CONSTRAINT FK_Products_Category FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID),
  CONSTRAINT UQ_Products_SKU UNIQUE (SKU),
  CONSTRAINT CK_Products_Price CHECK (UnitPrice > 0)
);
"""
    },
    "AuditLog": {
        "name": "AuditLog",
        "object_type": "table",
        "description": "System-versioned temporal audit table with XML column",
        "column_count": 8,
        "row_count": 150000000,
        "has_triggers": False,
        "index_count": 2,
        "fk_count": 0,
        "check_count": 0,
        "code": """
CREATE TABLE [dbo].[AuditLog] (
  [AuditID]      BIGINT          IDENTITY(1,1) NOT NULL,
  [TableName]    NVARCHAR(128)   NOT NULL,
  [Action]       NVARCHAR(10)    NOT NULL,
  [RecordID]     INT             NOT NULL,
  [ChangedDate]  DATETIME2       GENERATED ALWAYS AS ROW START NOT NULL,
  [EndDate]      DATETIME2       GENERATED ALWAYS AS ROW END NOT NULL,
  [ChangedBy]    NVARCHAR(128)   NULL DEFAULT SUSER_SNAME(),
  [ChangeDetail] XML             NULL,
  PERIOD FOR SYSTEM_TIME (ChangedDate, EndDate),
  CONSTRAINT PK_AuditLog PRIMARY KEY CLUSTERED (AuditID)
) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.AuditLogHistory));
"""
    },
    "GeoLocations": {
        "name": "GeoLocations",
        "object_type": "table",
        "description": "Geospatial table with geography type and spatial index — needs major rework",
        "column_count": 7,
        "row_count": 320000,
        "has_triggers": True,
        "index_count": 3,
        "fk_count": 1,
        "check_count": 0,
        "code": """
CREATE TABLE [dbo].[GeoLocations] (
  [LocationID]   INT            IDENTITY(1,1) NOT NULL,
  [LocationName] NVARCHAR(200)  NOT NULL,
  [GeoPoint]     GEOGRAPHY      NOT NULL,
  [GeoArea]      GEOMETRY       NULL,
  [Altitude]     FLOAT          NULL,
  [RegionCode]   NVARCHAR(10)   NOT NULL,
  [LastUpdated]  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_GeoLocations PRIMARY KEY CLUSTERED (LocationID),
  CONSTRAINT FK_Geo_Region FOREIGN KEY (RegionCode) REFERENCES Regions(RegionCode)
);
CREATE SPATIAL INDEX SIX_GeoLocations_Point ON [dbo].[GeoLocations](GeoPoint);
CREATE TRIGGER trg_Geo_Validate ON [dbo].[GeoLocations] AFTER INSERT AS
BEGIN
  IF EXISTS(SELECT 1 FROM inserted WHERE GeoPoint.STIsValid() = 0)
    RAISERROR('Invalid geography point', 16, 1);
END;
"""
    },
    "DocumentStore": {
        "name": "DocumentStore",
        "object_type": "table",
        "description": "FILESTREAM-backed document storage with hierarchyid — needs major rework",
        "column_count": 8,
        "row_count": 45000,
        "has_triggers": False,
        "index_count": 2,
        "fk_count": 0,
        "check_count": 0,
        "code": """
CREATE TABLE [dbo].[DocumentStore] (
  [DocID]        UNIQUEIDENTIFIER ROWGUIDCOL NOT NULL DEFAULT NEWSEQUENTIALID(),
  [FolderPath]   HIERARCHYID      NOT NULL,
  [FileName]     NVARCHAR(260)    NOT NULL,
  [FileContent]  VARBINARY(MAX) FILESTREAM NULL,
  [MimeType]     NVARCHAR(100)    NOT NULL,
  [FileSize]     BIGINT           NOT NULL,
  [UploadedBy]   NVARCHAR(128)    NOT NULL DEFAULT SUSER_SNAME(),
  [UploadDate]   DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_DocumentStore PRIMARY KEY CLUSTERED (DocID)
) FILESTREAM_ON DocFileGroup;
"""
    },
    "EmployeeSessions": {
        "name": "EmployeeSessions",
        "object_type": "table",
        "description": "Memory-optimized session table with sql_variant",
        "column_count": 6,
        "row_count": 5000,
        "has_triggers": False,
        "index_count": 1,
        "fk_count": 0,
        "check_count": 0,
        "code": """
CREATE TABLE [dbo].[EmployeeSessions] (
  [SessionID]    INT            IDENTITY(1,1) NOT NULL,
  [EmployeeID]   INT            NOT NULL,
  [LoginTime]    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  [SessionData]  SQL_VARIANT    NULL,
  [IPAddress]    NVARCHAR(45)   NOT NULL,
  [IsExpired]    BIT            NOT NULL DEFAULT 0,
  CONSTRAINT PK_EmpSessions PRIMARY KEY NONCLUSTERED (SessionID)
) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
"""
    },
}

# ═════════════════════════════════════════════════════════════════════════════
# ALL OBJECTS COMBINED — single lookup used by the API
# ═════════════════════════════════════════════════════════════════════════════
ALL_OBJECTS = {}
for _k, _v in STORED_PROCEDURES.items():
    ALL_OBJECTS[_k] = {**_v, "object_type": "stored_procedure"}
for _k, _v in SQL_VIEWS.items():
    ALL_OBJECTS[_k] = _v
for _k, _v in SQL_UDFS.items():
    ALL_OBJECTS[_k] = _v
for _k, _v in SQL_TABLES.items():
    ALL_OBJECTS[_k] = _v

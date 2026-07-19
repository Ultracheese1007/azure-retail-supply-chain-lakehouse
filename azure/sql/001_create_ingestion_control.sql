-- Ingestion control table for metadata-driven, watermark-based incremental loading.
--
-- The ADF metadata pipeline reads active rows from this table, copies each entity,
-- and updates last_watermark_value ONLY after the copy for that entity succeeds.
-- A failed copy leaves the previous watermark untouched, so the next run retries
-- the same range rather than skipping data.
--
-- Written for Azure SQL Database. The script is idempotent: it can be run
-- repeatedly without error and re-seeds metadata via MERGE.

-- ----------------------------------------------------------------------------
-- Schema
-- ----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'control')
    EXEC('CREATE SCHEMA control');
GO

-- ----------------------------------------------------------------------------
-- Control table
-- ----------------------------------------------------------------------------
IF OBJECT_ID('control.ingestion_metadata', 'U') IS NULL
BEGIN
    CREATE TABLE control.ingestion_metadata
    (
        entity_name            NVARCHAR(128)  NOT NULL,
        source_schema          NVARCHAR(128)  NOT NULL,
        source_table           NVARCHAR(128)  NOT NULL,
        load_type              NVARCHAR(20)   NOT NULL,   -- 'full' | 'incremental'
        watermark_column       NVARCHAR(128)  NULL,       -- NULL for full loads
        target_path            NVARCHAR(400)  NOT NULL,   -- relative ADLS path, e.g. retail/orders
        file_format            NVARCHAR(20)   NOT NULL,   -- 'parquet'
        is_active              BIT            NOT NULL CONSTRAINT DF_ingest_is_active DEFAULT (1),
        last_watermark_value   DATETIME2(6)   NULL,       -- previous successful high watermark
        last_successful_load   DATETIME2(6)   NULL,       -- when the last successful copy completed
        CONSTRAINT PK_ingestion_metadata PRIMARY KEY (entity_name),
        CONSTRAINT CK_ingestion_load_type CHECK (load_type IN ('full', 'incremental')),
        CONSTRAINT CK_ingestion_watermark
            CHECK (load_type = 'full' OR watermark_column IS NOT NULL)
    );
END
GO

-- ----------------------------------------------------------------------------
-- Seed / reconcile metadata (idempotent MERGE).
-- Keeps last_watermark_value and last_successful_load intact for existing rows
-- so re-running this script never rewinds progress.
-- ----------------------------------------------------------------------------
WITH seed (entity_name, source_schema, source_table, load_type, watermark_column, target_path, file_format, is_active) AS
(
    SELECT 'customers',        'retail', 'customers',        'incremental', 'updated_at',      'retail/customers',        'parquet', 1 UNION ALL
    SELECT 'products',         'retail', 'products',         'incremental', 'updated_at',      'retail/products',         'parquet', 1 UNION ALL
    SELECT 'warehouses',       'retail', 'warehouses',       'full',        NULL,              'retail/warehouses',       'parquet', 1 UNION ALL
    SELECT 'orders',           'retail', 'orders',           'incremental', 'order_timestamp', 'retail/orders',           'parquet', 1 UNION ALL
    SELECT 'order_items',      'retail', 'order_items',      'full',        NULL,              'retail/order_items',      'parquet', 1 UNION ALL
    SELECT 'inventory_events', 'retail', 'inventory_events', 'incremental', 'event_timestamp', 'retail/inventory_events', 'parquet', 1
)
MERGE control.ingestion_metadata AS target
USING seed AS source
    ON target.entity_name = source.entity_name
WHEN MATCHED THEN UPDATE SET
    target.source_schema    = source.source_schema,
    target.source_table     = source.source_table,
    target.load_type        = source.load_type,
    target.watermark_column = source.watermark_column,
    target.target_path      = source.target_path,
    target.file_format      = source.file_format,
    target.is_active        = source.is_active
WHEN NOT MATCHED BY TARGET THEN
    INSERT (entity_name, source_schema, source_table, load_type, watermark_column, target_path, file_format, is_active)
    VALUES (source.entity_name, source.source_schema, source.source_table, source.load_type,
            source.watermark_column, source.target_path, source.file_format, source.is_active);
GO

-- ----------------------------------------------------------------------------
-- Watermark update procedure.
-- Called by the ADF pipeline after a successful copy for one entity, passing the
-- high watermark that was actually copied. Never called on failure.
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE control.update_watermark
    @entity_name          NVARCHAR(128),
    @new_watermark_value  DATETIME2(6)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE control.ingestion_metadata
    SET last_watermark_value = @new_watermark_value,
        last_successful_load = SYSUTCDATETIME()
    WHERE entity_name = @entity_name;
END
GO

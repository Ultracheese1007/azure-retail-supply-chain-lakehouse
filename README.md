# Azure Retail & Supply Chain Lakehouse

**Status:** In development

A portfolio data engineering project that builds a trustworthy Lakehouse for a
multi-warehouse e-commerce retailer, covering customer-history management, sales
analysis, and inventory-movement analysis.

This repository is being built incrementally. Each milestone is a small,
reviewable change; the sections below describe what is *planned*. Nothing here
should be read as already implemented, deployed, or validated — see
**Known limitations**.

## Business problem

A multi-warehouse e-commerce retailer holds operational data across several
systems: a customer master that changes over time (addresses, contact details,
status), a product catalog, warehouse locations, order headers and line items,
and a continuous stream of inventory movements.

Reporting directly against these operational systems is slow, couples analytics
to transactional load, and gives no reliable view of history — for example, what
a customer's attributes were at the time an order was placed. The goal is a
Lakehouse that:

- preserves customer history so analysis can be done "as of" any point in time;
- produces consistent sales facts across orders and line items;
- tracks inventory movements per warehouse for stock-change analysis;
- makes data-quality expectations explicit and checkable.

## Proposed architecture

A Medallion (Bronze → Silver → Gold) design on Azure Databricks with Delta Lake:

```
Source entities (seeded/generated for this portfolio)
        |
        v
Bronze   raw ingestion, schema captured, minimal transformation
        |
        v
Silver   cleaning, typing, deduplication, validation, customer SCD Type 2
        |
        v
Gold     star schema (dimensions + facts) and analytics views
```

The project targets Databricks with managed storage so it can run without a
provisioned Azure Data Factory / on-prem source system; ingestion is simulated
from generated source data rather than a live extract.

## Planned source entities

| Entity | Description | Planned modelling role |
|--------|-------------|------------------------|
| `customers` | Customer master; attributes change over time | SCD Type 2 dimension |
| `products` | Product catalog | Dimension |
| `warehouses` | Warehouse / fulfilment locations | Dimension |
| `orders` | Order headers | Sales fact source |
| `order_items` | Order line items | Sales fact source |
| `inventory_events` | Append-only inventory movements per warehouse | Inventory-movement fact source |

## Planned layers

**Bronze** — raw landing for all six entities with ingestion metadata and
preserved source schema; no business logic.

**Silver** — cleaned and typed tables: deduplication, null/value validation,
standardisation, and Slowly Changing Dimension Type 2 history for `customers`.

**Gold** — a star schema intended to include customer, product and warehouse
dimensions, a sales fact derived from orders and order items, and an
inventory-movement fact derived from inventory events, plus a small set of
analytics views.

## MVP scope

The minimum end-to-end slice this project aims to deliver:

- generated source data for the six entities, including at least one customer
  attribute change to exercise history tracking;
- Bronze ingestion of all six entities;
- Silver layer with customer SCD Type 2 and basic validation;
- a Gold star schema with one sales fact and one inventory-movement fact, joined
  to point-in-time customer history;
- a set of data-quality checks (uniqueness, referential integrity, accepted
  values, business rules);
- a Databricks workflow that runs the layers in dependency order.

Anything beyond this slice is out of scope for the MVP.

## Roadmap

The project is built in milestones, each committed separately:

1. Project structure and development configuration *(current)*
2. Managed Databricks initialization and generated source data
3. Bronze ingestion and Silver transformations, including customer SCD Type 2
4. Gold star schema and point-in-time dimensional joins
5. Data-quality checks
6. Workflow orchestration
7. Documentation and architecture decision records

The roadmap may be adjusted as the work progresses.

## Known limitations

- The project uses generated, portfolio-scale data, not a production workload.
- There is no live source system: ingestion is simulated from generated data
  rather than extracted from an operational database or Azure Data Factory.
- Infrastructure is not provisioned as code; the project targets managed
  Databricks storage for portability.
- No performance or reliability figures are claimed; none have been measured.
- The design decisions here are for a portfolio context and are not intended as
  a production reference architecture.

## Development

Requires Python 3.11. Development tooling (linting via ruff, tests via pytest)
is configured in `pyproject.toml`.

## License

To be added.

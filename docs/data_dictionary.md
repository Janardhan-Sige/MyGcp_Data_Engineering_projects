# Data Dictionary

## Fact: fct_order_line

| Column | Type | Business meaning | Data quality expectation |
|---|---|---|---|
| order_id | STRING | Business order identifier | Required |
| order_line_id | STRING | Unique order-line key | Required / deduplicated |
| customer_id | STRING | Customer business key | Must exist in dimension |
| product_id | STRING | Product business key | Must exist in dimension |
| order_ts | TIMESTAMP | Business event time | Required |
| quantity | INT64 | Units ordered | Greater than zero |
| unit_price | NUMERIC | Unit selling price | Non-negative |
| discount_amount | NUMERIC | Commercial discount | Non-negative |
| tax_amount | NUMERIC | Tax amount | Non-negative |
| gross_amount | NUMERIC | Quantity multiplied by unit price | Reconciles to source |
| net_amount | NUMERIC | Gross minus discount plus tax | Reconciles to source |
| status | STRING | Order lifecycle state | Controlled values |
| payment_method | STRING | Payment instrument | Controlled values |
| channel | STRING | Sales channel | Controlled values |
| city | STRING | Fulfilment/customer city | Standardized |
| updated_at | TIMESTAMP | Source update timestamp | Used for dedupe |
| source_system | STRING | Originating system | Required |

## SCD Type 2: dim_customer

The dimension preserves historical versions using effective_from, effective_to, is_current and record_hash. This supports historical point-in-time analysis.

## Operational tables

retail_ops.pipeline_run_audit records source, accepted, rejected and duplicate counts, execution status, target table, Dataflow job ID and error information.

retail_ops.rejected_order_lines stores records failing business-quality rules instead of silently dropping them.
"""Production batch Dataflow pipeline for the retail order platform.

Cloud Storage CSV -> parse/type -> validate -> BigQuery raw landing.

Malformed records are emitted to a dead-letter output instead of being
silently lost. Downstream SQL owns deduplication, dimensional modeling
and marts.
"""

from __future__ import annotations

import argparse
import csv
import io
from typing import Iterable

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

REQUIRED_FIELDS = {
    "order_id", "order_line_id", "customer_id", "order_ts", "product_id",
    "quantity", "unit_price", "discount_amount", "tax_amount",
    "gross_amount", "net_amount", "status", "payment_method", "channel",
    "city", "updated_at", "source_system",
}


class ParseValidateOrder(beam.DoFn):
    """Convert CSV lines to typed records and tag invalid rows."""

    BAD = "bad"

    def process(self, line: str) -> Iterable[dict]:
        try:
            row = next(csv.DictReader(io.StringIO(line)))
            missing = REQUIRED_FIELDS - set(row)
            if missing:
                raise ValueError(f"Missing columns: {sorted(missing)}")

            if not row["order_id"] or not row["order_line_id"]:
                raise ValueError("Business key is missing")

            quantity = int(row["quantity"])
            unit_price = float(row["unit_price"])
            if quantity <= 0 or unit_price < 0:
                raise ValueError("Invalid quantity or unit_price")

            yield {
                **row,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": float(row["discount_amount"]),
                "tax_amount": float(row["tax_amount"]),
                "gross_amount": float(row["gross_amount"]),
                "net_amount": float(row["net_amount"]),
            }
        except (ValueError, TypeError, StopIteration) as exc:
            yield beam.pvalue.TaggedOutput(
                self.BAD,
                {"raw_record": line, "error_message": str(exc)},
            )


def run(argv=None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--dead_letter_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)

    order_schema = {
        "fields": [
            {"name": "order_id", "type": "STRING"},
            {"name": "order_line_id", "type": "STRING"},
            {"name": "customer_id", "type": "STRING"},
            {"name": "order_ts", "type": "TIMESTAMP"},
            {"name": "product_id", "type": "STRING"},
            {"name": "quantity", "type": "INTEGER"},
            {"name": "unit_price", "type": "NUMERIC"},
            {"name": "discount_amount", "type": "NUMERIC"},
            {"name": "tax_amount", "type": "NUMERIC"},
            {"name": "gross_amount", "type": "NUMERIC"},
            {"name": "net_amount", "type": "NUMERIC"},
            {"name": "status", "type": "STRING"},
            {"name": "payment_method", "type": "STRING"},
            {"name": "channel", "type": "STRING"},
            {"name": "city", "type": "STRING"},
            {"name": "updated_at", "type": "TIMESTAMP"},
            {"name": "source_system", "type": "STRING"},
        ]
    }
    dead_letter_schema = {"fields": [
        {"name": "raw_record", "type": "STRING"},
        {"name": "error_message", "type": "STRING"},
    ]}

    with beam.Pipeline(options=options) as pipeline:
        parsed = (
            pipeline
            | "ReadLandingFiles" >> beam.io.ReadFromText(known_args.input)
            | "ParseAndValidate" >> beam.ParDo(ParseValidateOrder()).with_outputs(
                ParseValidateOrder.BAD, main="valid"
            )
        )

        parsed.valid | "WriteRawOrders" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=order_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        parsed[ParseValidateOrder.BAD] | "WriteOrderDLQ" >> beam.io.WriteToBigQuery(
            known_args.dead_letter_table,
            schema=dead_letter_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    run()

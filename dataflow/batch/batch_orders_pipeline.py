"""Batch Dataflow pipeline: Cloud Storage CSV -> BigQuery.

Interview concepts covered:
1. Beam PipelineOptions.
2. Reading files from Cloud Storage.
3. PCollection transformations.
4. Data cleansing in Python.
5. Writing structured records to BigQuery.
"""

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow.common.transforms import clean_order, parse_csv_order


class ParseAndCleanOrder(beam.DoFn):
    """Parse a CSV row and apply reusable cleaning logic."""

    def process(self, line: str):
        # The first row is the CSV header and must not become a data record.
        if line.startswith("order_id,"):
            return

        try:
            yield clean_order(parse_csv_order(line))
        except (ValueError, IndexError) as exc:
            # In production, send malformed rows to a dead-letter table.
            # For learning, we simply log the rejected record.
            print(f"Rejected row: {line}. Reason: {exc}")


def run(argv=None):
    """Build and execute the batch pipeline."""

    parser = argparse.ArgumentParser()

    # These arguments make the same code reusable across environments.
    parser.add_argument("--input", required=True, help="GCS CSV input path")
    parser.add_argument("--output_table", required=True, help="BigQuery table")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)

    # BigQuery schema is explicit so interviewers can see the target contract.
    schema = {
        "fields": [
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_ts", "type": "DATETIME", "mode": "REQUIRED"},
            {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "unit_price", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_amount", "type": "FLOAT", "mode": "NULLABLE"},
        ]
    }

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read CSV From GCS" >> beam.io.ReadFromText(known_args.input)
            | "Parse And Clean Orders" >> beam.ParDo(ParseAndCleanOrder())
            | "Write Orders To BigQuery"
            >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()

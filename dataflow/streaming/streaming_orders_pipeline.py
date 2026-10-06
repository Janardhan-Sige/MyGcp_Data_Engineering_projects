"""Streaming Dataflow pipeline: Pub/Sub -> BigQuery.

This demonstrates the core streaming pattern:
Pub/Sub message -> decode -> Python transformation -> BigQuery.

For production systems, add dead-letter handling, monitoring, data-quality
checks, idempotency strategy, and an appropriate windowing/trigger design.
"""

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from dataflow.common.transforms import parse_event


class ParseEvent(beam.DoFn):
    """Convert a Pub/Sub message into a BigQuery-ready dictionary."""

    def process(self, message: bytes):
        try:
            yield parse_event(message)
        except Exception as exc:
            # A production pipeline should route invalid events to a DLQ.
            print(f"Invalid streaming event. Reason: {exc}")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)

    # Streaming must be explicitly enabled for an unbounded Pub/Sub source.
    options.view_as(StandardOptions).streaming = True

    schema = {
        "fields": [
            {"name": "order_id", "type": "STRING"},
            {"name": "customer_id", "type": "STRING"},
            {"name": "product_id", "type": "STRING"},
            {"name": "quantity", "type": "INTEGER"},
            {"name": "unit_price", "type": "FLOAT"},
            {"name": "order_amount", "type": "FLOAT"},
            {"name": "event_type", "type": "STRING"},
            {"name": "event_ts", "type": "TIMESTAMP"},
            {"name": "city", "type": "STRING"},
        ]
    }

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read From PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            )
            | "Parse Streaming Events" >> beam.ParDo(ParseEvent())
            | "Write Streaming Events To BigQuery"
            >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()

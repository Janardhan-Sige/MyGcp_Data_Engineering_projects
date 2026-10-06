"""Production streaming Dataflow pipeline: Pub/Sub -> BigQuery.

Valid events use event-time windows with bounded lateness. Invalid payloads
are written to a quarantine table. BigQuery Storage Write API is used for
the valid streaming sink.
"""

import argparse
import json

import apache_beam as beam
from apache_beam import window
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


class ParseAndValidate(beam.DoFn):
    BAD = "bad"

    def process(self, message: bytes):
        try:
            row = json.loads(message.decode("utf-8"))
            required = ["event_id", "order_id", "customer_id", "product_id",
                        "event_ts", "event_type"]
            missing = [field for field in required if not row.get(field)]
            if missing:
                raise ValueError(f"Missing fields: {missing}")

            row["quantity"] = int(row["quantity"])
            row["unit_price"] = float(row["unit_price"])
            row["order_amount"] = round(row["quantity"] * row["unit_price"], 2)

            event_timestamp = beam.utils.timestamp.Timestamp.from_rfc3339(
                row["event_ts"]
            )
            yield beam.window.TimestampedValue(row, event_timestamp)
        except Exception as exc:
            yield beam.pvalue.TaggedOutput(
                self.BAD,
                {"payload": message.decode("utf-8", errors="replace"),
                 "error_message": str(exc)},
            )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--dead_letter_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).streaming = True

    event_schema = {"fields": [
        {"name": "event_id", "type": "STRING"},
        {"name": "order_id", "type": "STRING"},
        {"name": "customer_id", "type": "STRING"},
        {"name": "product_id", "type": "STRING"},
        {"name": "event_type", "type": "STRING"},
        {"name": "event_ts", "type": "TIMESTAMP"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "unit_price", "type": "FLOAT"},
        {"name": "order_amount", "type": "FLOAT"},
        {"name": "city", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "schema_version", "type": "STRING"},
    ]}
    dlq_schema = {"fields": [
        {"name": "payload", "type": "STRING"},
        {"name": "error_message", "type": "STRING"},
    ]}

    with beam.Pipeline(options=options) as pipeline:
        parsed = (
            pipeline
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            )
            | "ParseValidateEvents" >> beam.ParDo(ParseAndValidate()).with_outputs(
                ParseAndValidate.BAD, main="valid"
            )
        )

        valid = parsed.valid | "EventTimeWindows" >> beam.WindowInto(
            window.FixedWindows(60),
            allowed_lateness=300,
            trigger=beam.trigger.AfterWatermark(
                early=beam.trigger.AfterProcessingTime(30),
                late=beam.trigger.AfterCount(100),
            ),
            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
        )

        valid | "WriteEvents" >> WriteToBigQuery(
            known_args.output_table,
            schema=event_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )

        parsed[ParseAndValidate.BAD] | "WriteEventDLQ" >> WriteToBigQuery(
            known_args.dead_letter_table,
            schema=dlq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
        )


if __name__ == "__main__":
    run()

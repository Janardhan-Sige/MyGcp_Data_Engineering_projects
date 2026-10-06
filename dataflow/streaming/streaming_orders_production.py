"""Production-style streaming Dataflow pipeline.

Pattern:
Pub/Sub -> parse/validate -> event-time timestamping -> fixed windows
-> BigQuery Storage Write API -> operationally observable raw events.

The pipeline intentionally separates invalid messages from the valid stream.
For a production deployment, the invalid branch can also be mirrored to a
Pub/Sub dead-letter topic for replay.
"""

import argparse
import json

import apache_beam as beam
from apache_beam import window
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


class ParseAndValidate(beam.DoFn):
    """Parse JSON and emit valid rows while tagging bad events."""

    BAD = "bad"

    def process(self, message: bytes):
        try:
            row = json.loads(message.decode("utf-8"))

            required = [
                "event_id",
                "order_id",
                "customer_id",
                "product_id",
                "event_ts",
                "event_type",
            ]
            missing = [field for field in required if not row.get(field)]
            if missing:
                raise ValueError("Missing fields: " + ", ".join(missing))

            row["quantity"] = int(row["quantity"])
            row["unit_price"] = float(row["unit_price"])
            row["order_amount"] = round(
                row["quantity"] * row["unit_price"], 2
            )

            # Beam uses the event timestamp for event-time windowing.
            event_seconds = (
                beam.utils.timestamp.Timestamp.from_rfc3339(
                    row["event_ts"]
                ).micros / 1_000_000
            )

            yield beam.window.TimestampedValue(row, event_seconds)

        except Exception as exc:
            yield beam.pvalue.TaggedOutput(
                self.BAD,
                {
                    "payload": message.decode("utf-8", errors="replace"),
                    "error_message": str(exc),
                },
            )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--dead_letter_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).streaming = True

    event_schema = {
        "fields": [
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
        ]
    }

    dead_letter_schema = {
        "fields": [
            {"name": "payload", "type": "STRING"},
            {"name": "error_message", "type": "STRING"},
        ]
    }

    with beam.Pipeline(options=options) as pipeline:
        parsed = (
            pipeline
            | "Read PubSub"
            >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            )
            | "Parse And Validate"
            >> beam.ParDo(ParseAndValidate()).with_outputs(
                ParseAndValidate.BAD,
                main="valid",
            )
        )

        valid = parsed.valid

        # Fixed event-time windows make late-arriving events explicit.
        windowed = valid | "One Minute Event Time Windows" >> beam.WindowInto(
            window.FixedWindows(60),
            allowed_lateness=300,
            trigger=beam.trigger.AfterWatermark(
                early=beam.trigger.AfterProcessingTime(30),
                late=beam.trigger.AfterCount(100),
            ),
            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
        )

        (
            windowed
            | "Write Valid Events"
            >> WriteToBigQuery(
                known_args.output_table,
                schema=event_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        )

        (
            parsed[ParseAndValidate.BAD]
            | "Write Dead Letter Events"
            >> WriteToBigQuery(
                known_args.dead_letter_table,
                schema=dead_letter_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        )


if __name__ == "__main__":
    run()

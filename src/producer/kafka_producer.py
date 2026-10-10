import argparse
import csv
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.producer.config import ProducerConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce-producer")

_SHUTDOWN = False


def _handle_signal(signum, frame):
    global _SHUTDOWN
    logger.info("Received signal %s — shutting down gracefully...", signum)
    _SHUTDOWN = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def load_csv_rows(csv_path: str) -> Iterator[Dict[str, Any]]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def row_to_json(row: Dict[str, Any]) -> str:
    payload = {
        "InvoiceNo": row.get("InvoiceNo", ""),
        "StockCode": row.get("StockCode", ""),
        "Description": row.get("Description", ""),
        "Quantity": int(row["Quantity"]) if row.get("Quantity") not in (None, "") else None,
        "InvoiceDate": row.get("InvoiceDate", ""),
        "UnitPrice": float(row["UnitPrice"]) if row.get("UnitPrice") not in (None, "") else None,
        "CustomerID": row.get("CustomerID", "") if row.get("CustomerID") not in (None, "") else None,
        "Country": row.get("Country", ""),
    }
    return json.dumps(payload, ensure_ascii=False)


def create_producer(config: ProducerConfig) -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        client_id=config.client_id,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        acks="all",
        retries=3,
        retry_backoff_ms=500,
        max_block_ms=10000,
        linger_ms=10,
    )
    logger.info("Kafka producer connected to %s", config.bootstrap_servers)
    return producer


def on_send_success(record_metadata):
    logger.debug(
        "Delivered to topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(excp):
    logger.error("Failed to deliver message: %s", excp)


def stream(config: ProducerConfig) -> None:
    producer = create_producer(config)
    sent = 0
    failed = 0

    logger.info("Streaming CSV: %s", config.csv_file_path)
    logger.info("Topic: %s | Delay: %.3fs | Max messages: %s",
                config.topic, config.stream_delay_seconds,
                "unlimited" if config.is_unlimited else config.max_messages)

    try:
        for row in load_csv_rows(config.csv_file_path):
            if _SHUTDOWN:
                logger.info("Shutdown flag set — stopping stream.")
                break

            if not config.is_unlimited and sent >= config.max_messages:
                logger.info("Reached max messages (%d) — stopping.", config.max_messages)
                break

            try:
                value = row_to_json(row)
                key = row.get("InvoiceNo", None)
                future = producer.send(config.topic, key=key, value=value)
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)
                sent += 1

                if sent % 1000 == 0:
                    logger.info("Published %d messages...", sent)

                if config.stream_delay_seconds > 0:
                    time.sleep(config.stream_delay_seconds)

            except (KafkaError, Exception) as exc:
                failed += 1
                logger.error("Error publishing row %d: %s", sent, exc)
                if config.stream_delay_seconds > 0:
                    time.sleep(config.stream_delay_seconds)

    except FileNotFoundError as exc:
        logger.error("%s", exc)
        raise
    finally:
        logger.info("Flushing producer...")
        producer.flush()
        producer.close(timeout=10)
        logger.info("Producer closed. Sent=%d | Failed=%d", sent, failed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="E-Commerce Kafka Producer — streams CSV transactions to Kafka."
    )
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=None, help="Kafka topic name")
    parser.add_argument("--csv", default=None, help="Path to the CSV dataset")
    parser.add_argument("--delay", type=float, default=None, help="Delay between messages (seconds)")
    parser.add_argument("--max-messages", type=int, default=None, help="Max messages to send (0 = unlimited)")
    parser.add_argument("--client-id", default=None, help="Kafka client id")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = ProducerConfig.from_overrides(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        csv_file_path=args.csv,
        stream_delay_seconds=args.delay,
        max_messages=args.max_messages,
        client_id=args.client_id,
    )

    logger.info("Starting E-Commerce Kafka Producer")
    logger.info("Config: bootstrap_servers=%s topic=%s csv=%s",
                config.bootstrap_servers, config.topic, config.csv_file_path)

    try:
        stream(config)
    except Exception as exc:
        logger.error("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

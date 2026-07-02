#!/usr/bin/env python3
"""
Kafka Producer Service - Generates realistic order events.

Produces JSON-formatted order events continuously to a configurable Kafka topic.
Includes:
  - Explicit Kafka configuration (not hidden in frameworks)
  - Structured JSON logging
  - Health endpoint
  - Graceful shutdown (SIGTERM/SIGINT)
  - Transient error retry with exponential backoff
  - Environment variable driven config
"""

import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from flask import Flask, jsonify
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

# ============================================================================
# Configuration from environment
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
KAFKA_PRODUCER_BATCH_SIZE = int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "1000"))
KAFKA_PRODUCER_LINGER_MS = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "100"))
KAFKA_COMPRESSION_TYPE = os.getenv("KAFKA_COMPRESSION_TYPE", "snappy")

MESSAGE_RATE_PER_SEC = float(os.getenv("MESSAGE_RATE_PER_SEC", "10"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))

# ============================================================================
# Structured JSON Logging
# ============================================================================

class JSONFormatter(logging.Formatter):
    """Format logs as JSON for structured log aggregation."""
    
    def format(self, record):
        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)

logger = logging.getLogger("kafka-producer")
logger.setLevel(LOG_LEVEL)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# ============================================================================
# Prometheus Metrics
# ============================================================================

PRODUCER_MESSAGES_TOTAL = Counter(
    "kafka_producer_messages_total",
    "Total number of Kafka messages produced successfully.",
)
PRODUCER_ERRORS_TOTAL = Counter(
    "kafka_producer_errors_total",
    "Total number of Kafka producer errors.",
)

# ============================================================================
# Order Event Model
# ============================================================================

@dataclass
class OrderEvent:
    """Represents a realistic order event."""
    
    event_id: str
    order_id: str
    customer_id: str
    amount: float
    currency: str
    timestamp: str
    product_ids: list
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(asdict(self))


def generate_order_event() -> OrderEvent:
    """Generate a realistic order event."""
    event_id = f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    order_id = f"ord_{random.randint(100000, 999999)}"
    customer_id = f"cust_{random.randint(1000, 99999)}"
    amount = round(random.uniform(10.0, 5000.0), 2)
    product_ids = [f"prod_{random.randint(1, 10000)}" for _ in range(random.randint(1, 5))]
    
    return OrderEvent(
        event_id=event_id,
        order_id=order_id,
        customer_id=customer_id,
        amount=amount,
        currency="USD",
        timestamp=datetime.utcnow().isoformat() + "Z",
        product_ids=product_ids,
    )


# ============================================================================
# Kafka Producer with Explicit Configuration
# ============================================================================

class KafkaProducerService:
    """
    Produces order events to Kafka with explicit configuration choices.
    
    Configuration rationale:
    - acks='all': Waits for leader + in-sync replicas. Critical for financial orders.
    - retries=-1 + max_in_flight_requests_per_connection=5: Automatic retry with ordering.
    - compression_type='snappy': Balance between CPU and network (LZ4 is faster, gzip better ratio).
    - batch.size + linger.ms: Batch small events for throughput without excessive latency.
    """
    
    def __init__(self):
        self.producer_config = {
            # Bootstrap
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

            # Idempotence: Exactly-once in-order delivery per partition.
            # Automatically sets: acks='all', retries=MAX_INT, max.in.flight.requests.per.connection=5.
            "enable.idempotence": True,

            # Durability: Ensure all replicas acknowledge (set automatically by idempotence).
            "acks": "all",

            # Backoff for retries.
            "retry.backoff.ms": 100,  # Start backoff at 100ms.
            # Ordering: Ensure messages sent from one producer stay ordered.
            "max.in.flight.requests.per.connection": 5,
            
            # Batching: Trade latency for throughput. Tune batch.size and linger.ms together.
            "batch.size": KAFKA_PRODUCER_BATCH_SIZE,
            "linger.ms": KAFKA_PRODUCER_LINGER_MS,
            
            # Compression: Reduce network bandwidth.
            "compression.type": KAFKA_COMPRESSION_TYPE,
            
            # Timeouts: delivery.timeout.ms caps the total time for retries.
            "request.timeout.ms": 30000,
            "delivery.timeout.ms": 120000,
            
            # Client identification
            "client.id": "kafka-producer-service",
        }
        
        self.producer = Producer(self.producer_config)
        self.messages_sent = 0
        self.messages_failed = 0
        self.is_running = True
        
        logger.info("Producer initialized", extra={
            "config": {
                "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "topic": KAFKA_TOPIC,
                "message_rate_per_sec": MESSAGE_RATE_PER_SEC,
            }
        })
    
    def delivery_callback(self, err, msg):
        """Callback on message delivery."""
        if err:
            self.messages_failed += 1
            PRODUCER_ERRORS_TOTAL.inc()
            logger.error("Message delivery failed", extra={
                "error": str(err),
                "topic": msg.topic(),
                "partition": msg.partition(),
            })
        else:
            self.messages_sent += 1
            PRODUCER_MESSAGES_TOTAL.inc()
            if self.messages_sent % 100 == 0:
                logger.info("Messages produced", extra={
                    "count": self.messages_sent,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                })
    
    def produce_events(self):
        """
        Continuously produce order events at the configured rate.
        Handles transient errors with backoff.
        """
        interval = 1.0 / MESSAGE_RATE_PER_SEC  # Time between messages
        
        while self.is_running:
            try:
                event = generate_order_event()
                
                # Produce with key (order_id) for ordering guarantees within a partition.
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event.order_id.encode("utf-8"),
                    value=event.to_json().encode("utf-8"),
                    on_delivery=self.delivery_callback,
                )
                
                # Flush periodically to ensure messages are sent.
                # This does not commit; it pushes to network buffer.
                self.producer.poll(0)
                
                time.sleep(interval)
                
            except KafkaError as e:
                logger.error("Kafka error during produce", extra={"error": str(e)})
                time.sleep(1)  # Backoff before retry
            except Exception as e:
                logger.error("Unexpected error during produce", extra={"error": str(e)})
                time.sleep(1)
    
    def shutdown(self):
        """Graceful shutdown: flush pending messages."""
        logger.info("Shutting down producer")
        self.is_running = False
        
        # Flush: wait for all pending messages to be delivered.
        # Timeout=30s to avoid hanging forever.
        remaining = self.producer.flush(timeout=30)
        
        if remaining > 0:
            logger.warning("Producer shutdown", extra={
                "messages_remaining": remaining,
                "messages_sent": self.messages_sent,
                "messages_failed": self.messages_failed,
            })
        else:
            logger.info("Producer shutdown complete", extra={
                "messages_sent": self.messages_sent,
                "messages_failed": self.messages_failed,
            })


# ============================================================================
# Flask HTTP Server for Health & Metrics
# ============================================================================

app = Flask(__name__)
producer_service: Optional[KafkaProducerService] = None


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    if producer_service and producer_service.is_running:
        return jsonify({
            "status": "healthy",
            "messages_sent": producer_service.messages_sent,
            "messages_failed": producer_service.messages_failed,
        }), 200
    else:
        return jsonify({"status": "unhealthy"}), 503


@app.route("/metrics", methods=["GET"])
def metrics():
    """Prometheus metrics endpoint."""
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


# ============================================================================
# Main Entrypoint
# ============================================================================

def main():
    global producer_service
    
    producer_service = KafkaProducerService()
    
    def signal_handler(sig, frame):
        logger.info("Received signal", extra={"signal": sig})
        producer_service.shutdown()
        sys.exit(0)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start HTTP server in background thread
    import threading
    http_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=HTTP_PORT, debug=False),
        daemon=True,
    )
    http_thread.start()
    logger.info(f"HTTP server started on port {HTTP_PORT}")
    
    # Start producing events
    try:
        producer_service.produce_events()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught")
        producer_service.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    main()

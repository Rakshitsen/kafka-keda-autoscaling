#!/usr/bin/env python3
"""
Kafka Consumer Service - Consumes and processes order events.

Consumes order events from a configurable consumer group, processes them
(with a simulated business logic delay), and commits offsets only after
successful processing.

Includes:
  - Explicit Kafka configuration (not hidden in frameworks)
  - Structured JSON logging with partition, offset, processing time
  - Health endpoint
  - Graceful shutdown (SIGTERM/SIGINT)
  - Configurable processing delay (for lag simulation)
  - Environment variable driven config
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Optional

from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING
from flask import Flask, jsonify
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

# ============================================================================
# Configuration from environment
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "order-processors")
KAFKA_AUTO_COMMIT = os.getenv("KAFKA_AUTO_COMMIT", "false").lower() == "true"

# Processing delay in seconds (simulates business logic like DB writes, API calls).
PROCESSING_DELAY_SECONDS = float(os.getenv("PROCESSING_DELAY_SECONDS", "0.1"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8081"))

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

logger = logging.getLogger("kafka-consumer")
logger.setLevel(LOG_LEVEL)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# ============================================================================
# Prometheus Metrics
# ============================================================================

CONSUMER_MESSAGES_TOTAL = Counter(
    "kafka_consumer_messages_total",
    "Total number of Kafka messages consumed and processed successfully.",
)
CONSUMER_ERRORS_TOTAL = Counter(
    "kafka_consumer_errors_total",
    "Total number of Kafka consumer or processing errors.",
)
CONSUMER_PROCESSING_DURATION_SECONDS = Histogram(
    "kafka_consumer_processing_duration_seconds",
    "Time spent processing a Kafka message.",
)

# ============================================================================
# Kafka Consumer with Explicit Configuration
# ============================================================================

class KafkaConsumerService:
    """
    Consumes order events from Kafka and processes them.
    
    Configuration rationale:
    - enable.auto.commit=false: Manual offset management ensures we only commit after success.
                                Prevents silent data loss if processing fails mid-transaction.
    - auto.offset.reset='earliest': Start from beginning if group is new. Useful for testing/replay.
    - session.timeout.ms: Rebalance if consumer hangs for >6s (not responding to heartbeats).
    - heartbeat.interval.ms: Send heartbeats every 3s to keep broker aware consumer is alive.
    - max.poll.interval.ms: Commit or poll every 5 minutes; else rebalance triggers.
                            Set high to allow long processing times; adjust with PROCESSING_DELAY_SECONDS.
    """
    
    def __init__(self):
        self.consumer_config = {
            # Bootstrap
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            
            # Consumer group: All consumers in a group share partitions.
            "group.id": KAFKA_CONSUMER_GROUP,
            
            # Offset Management: Manual commit ensures we commit AFTER processing.
            # If we auto-commit and processing fails, we lose the message.
            "enable.auto.commit": KAFKA_AUTO_COMMIT,
            
            # Session: Consumer is considered dead if no heartbeat for session.timeout.
            # Broker will trigger rebalance and reassign partitions.
            "session.timeout.ms": 6000,
            
            # Heartbeat: Send heartbeats every 3s to prove liveness.
            "heartbeat.interval.ms": 3000,
            
            # Poll interval: Consumer must poll or commit within this time.
            # Set high if processing is slow. Adjust relative to PROCESSING_DELAY_SECONDS.
            "max.poll.interval.ms": 300000,  # 5 minutes
            
            # Offset Reset: What to do if group has no committed offset.
            # 'earliest': Start from beginning (useful for new groups / replay).
            # 'latest': Start from end (ignore historical data).
            "auto.offset.reset": "earliest",
            
            # Isolation: Read only committed messages (not producer's in-flight writes).
            # Prevents consuming uncommitted/failed transactions.
            "isolation.level": "read_committed",
            
            # Client identification
            "client.id": f"kafka-consumer-service-{os.getenv('HOSTNAME', 'unknown')}",
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.topic = KAFKA_TOPIC
        
        # Subscribe to topic. Rebalancing is automatic.
        self.consumer.subscribe([self.topic])
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.current_lag = 0
        self.is_running = True
        
        logger.info("Consumer initialized", extra={
            "config": {
                "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "topic": self.topic,
                "group_id": KAFKA_CONSUMER_GROUP,
                "processing_delay_seconds": PROCESSING_DELAY_SECONDS,
            }
        })
    
    def process_event(self, event_data: dict, event_metadata: dict) -> bool:
        """
        Process an order event (simulated business logic).
        
        Args:
            event_data: Parsed JSON event.
            event_metadata: Kafka metadata (partition, offset, etc).
        
        Returns:
            True if processing succeeded, False otherwise.
        """
        try:
            # Simulate business processing: write to DB, call downstream API, etc.
            start_time = time.perf_counter()
            time.sleep(PROCESSING_DELAY_SECONDS)
            processing_duration = time.perf_counter() - start_time
            
            # Log with structured fields for lag tracking.
            logger.info("Event processed", extra={
                "event_id": event_data.get("event_id"),
                "order_id": event_data.get("order_id"),
                "amount": event_data.get("amount"),
                "partition": event_metadata.get("partition"),
                "offset": event_metadata.get("offset"),
                "processing_time_seconds": processing_duration,
            })
            
            self.messages_processed += 1
            CONSUMER_MESSAGES_TOTAL.inc()
            CONSUMER_PROCESSING_DURATION_SECONDS.observe(processing_duration)
            return True
            
        except Exception as e:
            CONSUMER_ERRORS_TOTAL.inc()
            logger.error("Event processing failed", extra={
                "event_id": event_data.get("event_id"),
                "error": str(e),
                "partition": event_metadata.get("partition"),
                "offset": event_metadata.get("offset"),
            })
            self.messages_failed += 1
            return False
    
    def run(self):
        """
        Main consumer loop: poll, process, commit.
        
        This explicit loop (instead of framework magic) shows Kafka's semantics clearly:
        1. Poll for message
        2. Process the message
        3. Commit offset ONLY if processing succeeded
        
        This is "at-least-once" semantics: a message is processed at least once
        before its offset is committed. If we crash, we'll reprocess from the
        last committed offset on restart.
        """
        logger.info("Consumer starting main loop")
        
        while self.is_running:
            try:
                # Poll: wait up to 1s for a message.
                # If no message arrives, returns None.
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message available; rebalancing may be happening.
                    continue
                
                if msg.error():
                    # Kafka-level error (not message content error).
                    # E.g., partition revoked, rebalancing, etc.
                    error = msg.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition; normal in low-traffic scenarios.
                        logger.debug("Partition EOF", extra={
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                        })
                    else:
                        logger.error("Consumer error", extra={
                            "error": str(error),
                            "code": error.code(),
                        })
                    continue
                
                # Parse message
                try:
                    event_data = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error("Failed to parse message JSON", extra={
                        "error": str(e),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    })
                    # Even on parse failure, we commit to avoid reprocessing
                    # (or skip commit if you want to replay; depends on strategy).
                    self.consumer.commit(asynchronous=False)
                    continue
                
                # Process the event
                event_metadata = {
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "topic": msg.topic(),
                }
                success = self.process_event(event_data, event_metadata)
                
                if success:
                    # Commit offset only after successful processing.
                    # This ensures at-least-once delivery.
                    self.consumer.commit(asynchronous=False)
                    
                    # Calculate lag for observability.
                    # Current offset: msg.offset()
                    # High-water mark (log-end-offset): msg.offset() + 1 (after we've read this msg).
                    # We'd need to query metrics to get true lag; here we approximate.
                    self.current_lag = max(0, self.current_lag - 1)
                else:
                    # Processing failed. Skip commit to retry on next run.
                    # In production, implement exponential backoff or DLQ here.
                    logger.warning("Skipping offset commit due to processing failure", extra={
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    })
                    # Optionally: send to dead-letter queue, or commit after N failures.
                
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt in main loop")
                break
            except Exception as e:
                logger.error("Unexpected error in main loop", extra={"error": str(e)})
                time.sleep(1)  # Backoff before retry
    
    def shutdown(self):
        """Graceful shutdown: finalize consumer."""
        logger.info("Shutting down consumer")
        self.is_running = False
        
        # Commit any pending offset before closing.
        self.consumer.commit(asynchronous=False)
        
        # Close consumer: revoke partitions, rebalance, etc.
        self.consumer.close()
        
        logger.info("Consumer shutdown complete", extra={
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
        })


# ============================================================================
# Flask HTTP Server for Health & Metrics
# ============================================================================

app = Flask(__name__)
consumer_service: Optional[KafkaConsumerService] = None


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    if consumer_service and consumer_service.is_running:
        return jsonify({
            "status": "healthy",
            "messages_processed": consumer_service.messages_processed,
            "messages_failed": consumer_service.messages_failed,
            "current_lag_estimate": consumer_service.current_lag,
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
    global consumer_service
    
    consumer_service = KafkaConsumerService()
    
    def signal_handler(sig, frame):
        logger.info("Received signal", extra={"signal": sig})
        consumer_service.shutdown()
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
    
    # Start consuming and processing events
    try:
        consumer_service.run()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught")
        consumer_service.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    main()

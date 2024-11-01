import os
import json
import logging
from uuid import UUID
from dotenv import load_dotenv
from loguru import logger
from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
from curl_cffi import requests
from typing import Dict, List

# Load environment variables from .env file
load_dotenv()

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_CONSUME = "collected_artefacts_dev"
KAFKA_TOPIC_PRODUCE = "collected_artefacts_dev"

# Database connection details
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# Proxy settings
PROXY_URL = os.getenv("PROXY_URL")

# Set up PostgreSQL connection
engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Set up Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'collected_artefacts_rss_group',
    'auto.offset.reset': 'latest'
})

# Set up Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Subscribe to the topic
consumer.subscribe([KAFKA_TOPIC_CONSUME])


def extract_data(soup: BeautifulSoup, artefactid: UUID, articleelement: Dict[str, List[str]], url: str) -> str:
    for key, value_list in articleelement.items():
        for value in value_list:
            tag = soup.find(key, class_=value)
            if tag:
                return tag.get_text(strip=True)
    raise ValueError(f"Text not found for artefact ID {artefactid} in the specified elements.")


def process_message(message_key, message_value):
    try:
        # Ignore messages that have already been processed (i.e., have the -scraped suffix in the key)
        if message_key.endswith("-scraped"):
            logger.info(f"Ignoring already processed message with key: {message_key}")
            return

        # Validate message content
        artefactid = message_value.get("artefactid")
        sourcetype = message_value.get("sourcetype")
        articleelement = message_value.get("articleelement")
        locator = message_value.get("locator")

        if not all([artefactid, sourcetype, articleelement, locator]) or not articleelement.get("div"):
            logger.error("Message missing required attributes or articleelement structure is invalid.")
            return

        # Process only if sourcetype is RSS
        if sourcetype != "RSS":
            logger.info(f"Ignoring artefact ID {artefactid}: sourcetype is not 'RSS'.")
            return

        # Fetch HTML content from locator URL
        response = requests.get(locator, impersonate="chrome", proxies={'http': PROXY_URL, 'https': PROXY_URL})

        if response.status_code != 200 or not response.text:
            logger.warning(f"Failed to fetch HTML for artefact ID {artefactid}: HTTP {response.status_code}.")
            return

        # Parse HTML and extract text using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        text_content = extract_data(soup, artefactid, articleelement, locator)

        # Update database with extracted content
        with engine.connect() as connection:
            update_query = text("UPDATE collected_artefacts SET rawcontent = :rawcontent WHERE artefactid = :artefactid")
            result = connection.execute(update_query, {"rawcontent": text_content, "artefactid": artefactid})
            connection.commit()  # Ensures the transaction is committed

            # Log number of rows affected to verify the update
            if result.rowcount > 0:
                logger.info(f"Updated rawcontent for artefact ID {artefactid}. Rows affected: {result.rowcount}")
            else:
                logger.warning(f"No rows updated for artefact ID {artefactid}. Check if artefact ID exists.")

        # Publish updated message to Kafka
        updated_message = {**message_value, "rawcontent": text_content}
        producer.produce(KAFKA_TOPIC_PRODUCE, key=f"{artefactid}-scraped", value=json.dumps(updated_message))
        producer.flush()

        logger.info(f"Successfully processed and updated artefact ID {artefactid}.")

    except Exception as e:
        logger.error(f"Error processing message for artefact ID {message_value.get('artefactid', 'unknown')}: {e}")

# Run the Kafka consumer
try:
    logger.info("Starting Kafka consumer...")
    while True:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            logger.error(f"Consumer error: {message.error()}")
            continue

        message_key = message.key().decode("utf-8") if message.key() else "No Key"
        message_value = json.loads(message.value().decode("utf-8"))

        logger.info(f"Received message with key: {message_key}")
        process_message(message_key, message_value)

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user.")

finally:
    consumer.close()
    logger.info("Kafka consumer closed.")

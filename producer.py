import datetime as dt
import json
import logging
import random
import time
import uuid

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:19092"]
TOPIC = "financial-transactions"
SEND_INTERVAL_SECONDS = 2
MISSING_LOCATION_RATE = 0.12

TRANSACTION_TYPES = ["achat", "remboursement", "transfert"]
PAYMENT_METHODS = ["carte_de_credit", "especes", "virement_bancaire", "erreur"]
CITIES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg",
    "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre",
    "Saint-Étienne", "Toulon",
]
STREETS = [
    "Rue de la République", "Rue de Paris", "Rue Auguste Delaune",
    "Rue Gustave Courbet", "Rue de Luxembourg", "Rue Fontaine",
    "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta",
    "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée",
    "Rue de la Villette", "Rue de la Pompe", "Rue Saint-Michel",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_transaction():
    return {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(TRANSACTION_TYPES),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": dt.datetime.now().isoformat(),
        "lieu": None if random.random() < MISSING_LOCATION_RATE else f"{random.choice(STREETS)}, {random.choice(CITIES)}",
        "moyen_paiement": random.choice(PAYMENT_METHODS),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2),
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(STREETS)}, {random.choice(CITIES)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com",
        },
    }


def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: json.dumps(v).encode("utf-8") if v else None,
            acks="all",
            retries=3,
        )
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return None


def send_transactions():
    producer = create_kafka_producer()
    if not producer:
        return

    try:
        while True:
            transaction = generate_transaction()
            # Keyed by user so one user's transactions stay in the same partition
            key = transaction["utilisateur"]["id_utilisateur"]

            producer.send(topic=TOPIC, key=key, value=transaction)
            logger.info(f"Sent transaction: {transaction['id_transaction']} for user: {key}")

            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception as e:
        logger.error(f"Error sending transaction: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    send_transactions()

import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='redpanda:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "client_tickets"

types = ["Technique", "Facturation", "Compte", "Livraison"]
priorites = ["Faible", "Moyenne", "Haute", "Critique"]

def generate_ticket():
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": f"C{random.randint(1000,9999)}",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "demande": random.choice([
            "Problème connexion",
            "Erreur paiement",
            "Demande remboursement",
            "Commande non reçue"
        ]),
        "type_demande": random.choice(types),
        "priorite": random.choice(priorites)
    }

while True:
    ticket = generate_ticket()
    producer.send(topic, ticket)
    producer.flush()

    print("Ticket envoyé :", ticket)

    time.sleep(2)
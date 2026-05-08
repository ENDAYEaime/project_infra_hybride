# Data Pipeline Streaming avec Redpanda

## Contexte

Dans un contexte de traitement de données en temps réel, ce projet vise à concevoir un pipeline capable de simuler, transporter et exploiter des flux de données de manière continue.

## Objectif

Mettre en place une architecture de streaming permettant de :
- Générer des données simulées
- Les transmettre via un système de messaging
- Les consommer et les traiter

---

## Architecture

Le pipeline repose sur les composants suivants :

- Producer : génère des tickets de manière aléatoire
- Redpanda : assure le transport des données (streaming)
- Consumer : consomme les messages produits
- Docker : conteneurisation de l’ensemble des services

---

## Diagramme du pipeline

---
config:
  layout: elk
---
flowchart LR
 subgraph onprem["On-premise"]
        sql["SQL Server<br><small>ERP / CRM</small>"]
        san["SAN<br><small>Logs, fichiers, IoT</small>"]
        ad["Active Directory<br><small>Gestion des identités</small>"]
  end
 subgraph ingestion["Ingestion & Streaming"]
        redpanda["Redpanda<br><small>Ingestion des flux IoT en temps réel<br>Transport via protocoles Kafka API</small>"]
  end
 subgraph traitement["Traitement"]
        spark["Apache Spark — AWS EMR<br><small>Nettoyage · Transformation · Enrichissement<br>Batch et streaming</small>"]
  end
 subgraph stockage["Stockage"]
        s3["Amazon S3 — Data Lake<br><small>Données brutes (raw) · Données transformées<br>Formats Parquet, JSON, CSV</small>"]
  end
 subgraph analyse["Analyse"]
        redshift["Amazon Redshift — Entrepôt de données<br><small>Requêtes analytiques SQL</small>"]
  end
 subgraph integration["Intégration SI"]
        dms["AWS DMS<br><small>Synchronisation SQL Server → Redshift / S3<br>Réplication continue (CDC)</small>"]
  end
 subgraph securite["Sécurité"]
        iam["AWS IAM + AWS Directory Service<br><small>Intégration Active Directory<br>Gestion centralisée des accès</small>"]
  end
 subgraph cloud["AWS Cloud"]
    direction TB
        ingestion
        traitement
        stockage
        analyse
        integration
        securite
  end
    onprem --> vpn["Connexion sécurisée — VPN IPSec / AWS Direct Connect"]
    vpn --> ingestion
    ingestion --> traitement
    traitement --> stockage
    stockage --> analyse
    analyse --> integration
    integration --> securite

     sql:::onprem
     san:::onprem
     ad:::onprem
     redpanda:::ingestion
     spark:::traitement
     s3:::stockage
     redshift:::analyse
     dms:::integration
     iam:::securite


    Structure du projet

    project/
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── consumer.py
│
├── docker-compose.yml
├── Dockerfile
└── README.md
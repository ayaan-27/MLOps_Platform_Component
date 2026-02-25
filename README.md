# Pace-ML

Pace-ML is an end-to-end **MLOps platform** built with a Flask REST API backend. It provides a unified, governed environment for the full machine learning lifecycle — from data ingestion, preprocessing, and feature engineering through automated model training, experiment tracking, explainability, versioning, and production deployment. Teams can collaborate on ML projects with fine-grained user and role management, while asynchronous job execution keeps long-running pipelines non-blocking and scalable.

---

## MLOps Lifecycle

Pace-ML covers every stage of the MLOps loop:

```
Data Ingestion → Profiling → PII Masking → Preprocessing → Feature Engineering
      → Augmentation → AutoML Training → Experiment Tracking (MLflow)
            → Model Hub (Versioning & Tagging) → Explainability
                  → Deployment → Monitoring & Governance
```

Each stage is exposed as a REST API, backed by an async job queue, and scoped to projects with role-based access control.

---

## Features

**Data Management**
- Dataset upload, versioning, and retrieval backed by AWS S3 with full metadata tracking
- Automatic statistical profiling via pandas-profiling
- PII detection and masking to ensure data privacy compliance before training

**Data Preparation**
- Configurable preprocessing pipelines: null/duplicate removal, encoding, scaling, binning, clipping, and math operations
- Feature engineering: polynomial features, datetime transformations, custom math ops, and multicollinearity removal
- Class imbalance handling via Variational Autoencoder (PyTorch) augmentation or traditional sampling methods

**Automated ML**
- Automated model selection across a wide library (scikit-learn, XGBoost, LightGBM, CatBoost) for classification and regression
- Bayesian hyperparameter tuning via BayTune/BTB with a scored leaderboard
- Experiment tracking and artifact storage backed by MLflow

**Model Governance**
- Centralized Model Hub with versioning, tagging, and retrieval
- Global model explainability via InterpretML's TabularExplainer
- Fairness evaluation via Fairlearn
- Full audit trail through session history and job logs

**Operations & Infrastructure**
- Asynchronous pipeline execution via Celery and RabbitMQ — long-running jobs don't block the API
- Model deployment from the hub to serving endpoints
- Project-scoped user and role management with JWT authentication
- Billing and license tracking for multi-tenant deployments

---

## Tech Stack

| Layer | Technology |
|---|---|
| API Framework | Flask + Flask-CORS |
| Database | PostgreSQL (via SQLAlchemy) |
| Task Queue | Celery + RabbitMQ (pika) |
| ML Tracking | MLflow |
| Object Storage | AWS S3 (boto3, s3fs) |
| AutoML | BayTune / BTB, scikit-learn, XGBoost, LightGBM, CatBoost |
| Deep Learning | PyTorch (VAE augmentation) |
| Explainability | InterpretML |
| Fairness | Fairlearn |
| Profiling | pandas-profiling |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
Pace-ML/
├── backend/
│   ├── app.py                   # Flask application entry point & all API routes
│   ├── src.py                   # Core ML pipeline orchestration
│   ├── wsgi.py                  # WSGI entry point
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── adminstration/           # User and role management
│   ├── augmentation/            # VAE and sampling-based data augmentation
│   ├── auto_ml/                 # AutoML: model definitions, training, leaderboard, scoring
│   ├── confs/                   # App configuration (config.ini, config.py)
│   ├── datasets/                # Dataset upload, versioning, retrieval
│   ├── db/                      # SQLAlchemy models and DB helper functions
│   ├── deploy/                  # Model deployment logic
│   ├── feature_eng/             # Feature engineering transformers and pipeline
│   ├── global_exp/              # Global model explainability
│   ├── jobs/                    # Celery tasks, job management, RabbitMQ publish/consume
│   ├── model_hub/               # Model storage, retrieval, and versioning
│   ├── pii/                     # PII masking utilities
│   ├── preprocessing/           # Preprocessing transformers and master pipeline
│   ├── profiling/               # Dataset profiling
│   ├── projects/                # Project management
│   ├── tests/                   # Unit tests (pytest)
│   ├── user_auth/               # JWT authentication
│   └── utils/                   # Logging, file I/O, MLflow utils
└── docker-compose.yml           # PostgreSQL, RabbitMQ, and MLflow services
```

---

## Getting Started

### Prerequisites

- Python 3.8+
- Docker & Docker Compose
- AWS credentials (for S3 artifact storage)

### 1. Clone the repository

```bash
git clone https://github.com/your-org/Pace-ML.git
cd Pace-ML
```

### 2. Configure the application

Edit `backend/confs/config.ini` with your database connection details and S3 bucket name:

```ini
[postgresql-local]
host = localhost
database = pace
user = pace_user
password = your_password

[dataset]
bucketname = your-s3-bucket
foldername = paceml
```

Set your AWS credentials as environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1
```

### 3. Start infrastructure services

```bash
docker-compose up -d
```

This starts PostgreSQL, RabbitMQ, and the MLflow tracking server.

### 4. Set up the database

```bash
cd backend
python db/create_tables.py
```

### 5. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 6. Run the Flask API

```bash
cd backend
python wsgi.py
```

The API will be available at `http://localhost:5000`.

---

## Running Tests

```bash
cd backend
pytest tests/unit_test/
```

---

## API Overview

The API is organized around the following resource groups, all exposed via `app.py`:

| Resource | Description |
|---|---|
| `POST /login` | Authenticate and retrieve a JWT token |
| `/datasets/*` | Upload, version, and retrieve datasets |
| `/projects/*` | Create and manage ML projects |
| `/preprocessing/*` | Configure and run preprocessing pipelines |
| `/feature_eng/*` | Run feature engineering steps |
| `/augmentation/*` | Augment datasets |
| `/auto_ml/*` | Trigger AutoML training runs and retrieve leaderboards |
| `/model_hub/*` | Browse, tag, and manage trained models |
| `/deploy/*` | Deploy models to serving |
| `/jobs/*` | Monitor async job status |
| `/admin/*` | User and role administration |

---

## Configuration

The `backend/confs/config.ini` file controls database connections, S3 bucket settings, MLflow tracking URI, and model artifact paths. The `backend/confs/config.py` module reads and exposes these values to the rest of the application.

---

## License

This project is proprietary. Please refer to your organization's licensing agreement.

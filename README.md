# UCM TFM - Complete Data Lakehouse Solution

A comprehensive data lakehouse solution built for the UCM Big Data Master's program, featuring a complete modern data platform with infrastructure as code, data orchestration, and interactive user interfaces.

## 🏗️ Project Overview

This repository contains a complete end-to-end data platform implementation with the following key components:

### 🌐 **Infrastructure as Code (Terraform)**
- Azure-based data lakehouse infrastructure
- Databricks workspace with Unity Catalog
- Azure Kubernetes Service (AKS) for containerized workloads
- Data Lake Gen2 storage (landing and lakehouse layers)
- Azure Key Vault for secrets management
- Automated resource group and access management

### ⚙️ **Data Processing & Orchestration**
- **Apache Airflow** deployed on AKS for workflow orchestration
- **Databricks Notebooks** for Bronze/Silver/Gold layer processing
- **ETL Libraries** with modular Bronze and Silver layer processors
- **Configuration-driven** data ingestion and transformation

### 🤖 **Interactive Data Interfaces**
- **Telegram Bot** - Natural language queries via Telegram
- **Streamlit Web UI** - Interactive web interface for data exploration
- **Databricks Genie AI** integration for SQL generation from natural language

## 📁 Repository Structure

```
ucm_tfm/
├── 📁 infrastructure/           # Terraform infrastructure as code
│   ├── main.tf                  # Main infrastructure definition
│   ├── variables.tf             # Infrastructure variables
│   ├── terraform.tfvars         # Environment-specific values
│   └── modules/                 # Reusable Terraform modules
│       ├── aks/                 # Azure Kubernetes Service
│       ├── databricks_workspace/ # Databricks workspace setup
│       ├── databricks_clusters/ # Compute clusters
│       ├── storage_account/     # Data Lake storage
│       ├── unity_catalog/       # Databricks Unity Catalog
│       └── key_vault/          # Azure Key Vault
├── 📁 airflow/                  # Apache Airflow orchestration
│   ├── dags/                    # Airflow DAGs
│   │   ├── databricks/          # Databricks job orchestration
│   │   └── commons/             # Shared utilities
│   └── images/                  # Custom Docker images
├── 📁 code/                     # Main application code
│   ├── src/                     # Source code
│   │   ├── databricks_telegram_bot/ # Interactive interfaces
│   │   └── libraries/           # Data processing libraries
│   │       ├── bronze_layer/    # Raw data ingestion
│   │       ├── silver_layer/    # Data transformation
│   │       └── gold_layer/      # Business intelligence layer
│   └── tests/                   # Test suites
├── 📁 databricks_notebooks/     # Databricks processing notebooks
│   ├── Bronze.py                # Bronze layer processing
│   ├── Silver.py                # Silver layer processing
│   └── Gold.py                  # Gold layer processing
├── 📁 config_files/             # Data processing configurations
│   ├── bronze/                  # Bronze layer configs
│   ├── silver/                  # Silver layer configs
│   └── gold/                    # Gold layer configs
└── 📁 scripts/                  # Utility scripts
    ├── aks_login.sh             # AKS authentication
    └── load_env.sh              # Environment setup
```

## 🚀 Key Features

### 🏗️ **Complete Infrastructure Automation**
- **Multi-Resource Group Setup**: Separate resource groups for lakehouse and Databricks
- **Azure Active Directory Integration**: Automated user and group management
- **Storage Architecture**: Landing zone and lakehouse with proper directory structure
- **Databricks Premium Workspace**: With Unity Catalog for data governance
- **AKS Cluster**: Kubernetes cluster for containerized workloads
- **Secrets Management**: Azure Key Vault with automated secret provisioning

### 📊 **Data Lakehouse Architecture**
- **Bronze Layer**: Raw data ingestion from various sources with schema evolution
- **Silver Layer**: Cleaned and transformed data with business logic
- **Gold Layer**: Aggregated data for analytics and reporting
- **Unity Catalog**: Centralized metadata and governance
- **Delta Lake**: ACID transactions and time travel capabilities

### 🔄 **Orchestration & Processing**
- **Airflow on Kubernetes**: Scalable workflow orchestration
- **Databricks Integration**: Native Databricks job scheduling
- **Configuration-Driven**: JSON-based processing configurations
- **Monitoring**: Comprehensive logging and error handling

### 🤖 **User Interfaces**
- **Natural Language Queries**: Ask questions in plain English
- **SQL Generation**: Automated SQL creation using Databricks Genie
- **Multi-Interface Support**: Telegram bot and web UI
- **Real-time Results**: Interactive data exploration

## 🛠️ Infrastructure Components

### Azure Resources Created

| Resource Type | Purpose | Configuration |
|---------------|---------|---------------|
| **Resource Groups** | Organization and access control | Lakehouse + Databricks separation |
| **Storage Accounts** | Data lake storage | Landing + Lakehouse with Gen2 |
| **Databricks Workspace** | Data processing platform | Premium tier with Unity Catalog |
| **AKS Cluster** | Container orchestration | 2-node cluster with auto-scaling |
| **Key Vault** | Secrets management | Access keys and sensitive data |
| **Access Connector** | Databricks-Storage integration | Managed identity access |
| **Unity Catalog** | Data governance | Metastore with external locations |

### Kubernetes Workloads

| Service | Description | Access |
|---------|-------------|--------|
| **Apache Airflow** | Workflow orchestration | Port-forward to 8080 |
| **MLflow** (Optional) | ML model management | Configurable deployment |

## 📈 Data Processing Workflows

### Bronze Layer Ingestion
```json
{
  "datasource": "internal_system",
  "dataset": "facturas",
  "source": {
    "format": "cloudFiles",
    "options": {
      "cloudFiles.format": "csv",
      "header": "true",
      "cloudFiles.inferColumnTypes": "true"
    }
  }
}
```

### Silver Layer Transformation
- **Data Quality**: Validation and cleaning
- **Business Logic**: Domain-specific transformations
- **Schema Evolution**: Automatic schema management
- **Error Handling**: Robust error recovery

### Available Datasets
- **Facturas** (Invoices)
- **Detalle Facturas** (Invoice Details)
- **Notas Credito** (Credit Notes)
- **Detalle Notas Credito** (Credit Note Details)
- **Clientes** (Customers)
- **Departamentos** (Departments)
- **Municipios** (Municipalities)

## 🚀 Getting Started

### Prerequisites
- Azure subscription with appropriate permissions
- Terraform >= 1.0
- Azure CLI
- kubectl
- Python 3.11+
- Docker (optional)

### 1. Infrastructure Deployment

```bash
# Login to Azure
az login

# Navigate to infrastructure directory
cd infrastructure/

# Initialize Terraform
terraform init

# Plan deployment
terraform plan

# Apply infrastructure
terraform apply
```

### 2. AKS Access Setup

```bash
# Configure kubectl for AKS
source ./scripts/aks_login.sh

# Verify cluster access
kubectl get nodes
```

### 3. Airflow Access

```bash
# Port forward to Airflow
kubectl port-forward svc/airflow-server-api-server 8080:8080

# Access at http://localhost:8080
```

### 4. Application Setup

```bash
# Navigate to code directory
cd code/

# Install dependencies
uv sync

# Configure environment
cp env.example .env
# Edit .env with your credentials

# Run Telegram bot
make run-bot

# Run Streamlit UI
make run-chat
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_WORKSPACE_URL` | Databricks workspace URL | ✅ |
| `DATABRICKS_ACCESS_TOKEN` | Personal access token | ✅ |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token | ✅ (for bot) |
| `TELEGRAM_ALLOWED_USERS` | Authorized user IDs | ✅ (for bot) |

### Infrastructure Variables

Key Terraform variables in `terraform.tfvars`:

```hcl
project = "ucmappinnova"
location = "West Europe"
databricks_location = "West Europe"
group_members = ["user@domain.com"]
users = ["user@domain.com"]
```

## 🎯 Use Cases

### Data Engineers
- Deploy complete data lakehouse infrastructure
- Configure ETL pipelines with Airflow
- Manage data quality and transformations
- Monitor data processing workflows

### Data Analysts
- Query data using natural language via Telegram/Web UI
- Explore datasets interactively
- Generate ad-hoc reports and insights
- Access processed data through Unity Catalog

### Data Scientists
- Access clean datasets from Silver/Gold layers
- Use Databricks notebooks for analysis
- Deploy models with MLflow integration
- Leverage compute clusters for heavy workloads

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Azure Cloud                          │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────────────────────┐ │
│  │   Landing Zone  │  │         Lakehouse               │ │
│  │   (Raw Data)    │  │  ┌───────┐ ┌───────┐ ┌───────┐  │ │
│  │                 │  │  │Bronze │ │Silver │ │ Gold  │  │ │
│  └─────────────────┘  │  └───────┘ └───────┘ └───────┘  │ │
│                       └─────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────┐ │
│  │            Databricks Workspace                    │ │
│  │  ┌─────────────────┐  ┌─────────────────────────┐   │ │
│  │  │ Unity Catalog   │  │    Compute Clusters     │   │ │
│  │  └─────────────────┘  └─────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              AKS Cluster                            │ │
│  │  ┌─────────────────┐  ┌─────────────────────────┐   │ │
│  │  │   Airflow       │  │      Other Services     │   │ │
│  │  └─────────────────┘  └─────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│               User Interfaces                           │
│  ┌─────────────────┐  ┌─────────────────────────────┐   │
│  │ Telegram Bot    │  │     Streamlit Web UI        │   │
│  │ (Natural Lang.) │  │   (Interactive Dashboard)   │   │
│  └─────────────────┘  └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## 🧪 Testing

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Test specific components
pytest tests/databricks_telegram_bot/
```

## 📝 Documentation

- **[Airflow Guide](airflow/README.md)** - Airflow deployment and access
- **[Code Documentation](code/README.md)** - Detailed application documentation
- **Infrastructure Modules** - Individual module documentation in `infrastructure/modules/`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

*Built with ❤️ for the UCM Big Data Master's Program - Complete Data Lakehouse Solution*

# Customer Churn Data Pipeline Documentation

## Project Overview

This project implements a comprehensive end-to-end data pipeline for customer churn prediction, demonstrating industry best practices for data engineering, machine learning operations, and automated pipeline orchestration.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Data Sources   │    │   Processing    │    │    Outputs      │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • CSV Files     │───▶│ • Ingestion     │───▶│ • Trained Models│
│ • APIs          │    │ • Validation    │    │ • Reports       │
│ • Kaggle        │    │ • Preparation   │    │ • Dashboards    │
│ • Databases     │    │ • Feature Eng.  │    │ • Versions      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Project Structure

```
AiImageDetection/
├── pipeline_data/                 # All pipeline outputs
│   ├── raw/                      # Raw ingested data
│   ├── processed/                # Cleaned data
│   ├── transformed/              # Feature-engineered data
│   ├── models/                   # Trained models and metadata
│   ├── reports/                  # Data quality and model reports
│   ├── logs/                     # Execution logs
│   ├── feature_store/            # Feature metadata and encoders
│   ├── versions/                 # Data version control
│   └── monitoring/               # Pipeline monitoring
├── data_ingestion/               # Data ingestion system
│   ├── config.py                # Configuration settings
│   ├── logger.py                # Logging utilities
│   ├── utils.py                 # Helper functions
│   ├── orchestrator.py          # Ingestion orchestrator
│   ├── ingestion_handlers.py    # Source-specific handlers
│   ├── main.py                  # Entry point
│   ├── examples.py              # Usage examples
│   └── requirements.txt         # Dependencies
├── churn_dataset/               # Source data
│   └── cell2cellholdout.csv    # Main dataset
├── CHURN.ipynb                  # Main pipeline notebook
├── pipeline_orchestrator.py    # Complete pipeline orchestrator
└── README.md                    # This documentation
```

## Features Implemented

### 1. ✅ Data Ingestion
- **Automated periodic fetching** (hourly/daily/weekly schedules)
- **Multiple source support** (local files, APIs, Kaggle datasets)
- **Comprehensive error handling** with retry logic
- **Detailed logging** for monitoring and debugging
- **Data integrity checks** with file hashing
- **Configurable source management**

**Key Files:**
- `data_ingestion/main.py` - Main ingestion entry point
- `data_ingestion/orchestrator.py` - Scheduling and coordination
- `data_ingestion/ingestion_handlers.py` - Source-specific logic

**Usage:**
```bash
# Run ingestion once
python -m data_ingestion.main run

# Run as scheduled service
python -m data_ingestion.main schedule

# Check status
python -m data_ingestion.main status
```

### 2. ✅ Raw Data Storage
- **Organized folder structure** partitioned by source, type, and timestamp
- **Metadata tracking** for all ingested files
- **Automatic backup creation** before overwriting
- **File size validation** and integrity checks
- **Timestamped storage** to prevent conflicts

**Folder Structure:**
```
pipeline_data/raw/
├── churn_data_20250820_143022.csv
├── ingestion_metadata_20250820_143022.json
└── image_data_20250820_143045/
```

### 3. ✅ Data Validation
- **Missing data analysis** with percentage thresholds
- **Data type validation** and inconsistency detection
- **Duplicate record identification** and handling
- **Range validation** and outlier detection using IQR method
- **Comprehensive quality scoring** (0-100 scale)
- **Automated quality reports** in JSON format

**Quality Metrics:**
- Data completeness percentage
- Type consistency checks
- Duplicate record percentage
- Statistical outlier analysis
- Overall quality score calculation

### 4. ✅ Data Preparation
- **Smart missing value handling**:
  - Median imputation for numerical features
  - Mode imputation for categorical features
  - Column removal for >50% missing data
- **Duplicate removal** with logging
- **Outlier handling** using IQR capping method
- **Comprehensive EDA** with automated visualizations
- **Statistical summaries** and correlation analysis

**Visualizations Generated:**
- Dataset overview statistics
- Missing data patterns
- Feature distributions
- Correlation heatmaps

### 5. ✅ Feature Engineering & Transformation
- **Automated target variable identification** or creation
- **Aggregated feature creation**:
  - Total spending/usage features
  - Average and sum calculations
  - High-value customer indicators
- **Smart categorical encoding**:
  - One-hot encoding for low cardinality (<= 10 unique values)
  - Label encoding for high cardinality
- **Feature scaling** using StandardScaler
- **Feature metadata tracking** with creation timestamps

### 6. ✅ Feature Store Implementation
- **Metadata management** for all engineered features
- **Versioned encoder/scaler storage** using joblib
- **Feature lineage tracking** from source to transformed
- **Retrieval API** for training and inference
- **Documentation** of feature creation logic

**Feature Store Structure:**
```json
{
  "feature_name": {
    "description": "Feature description",
    "source_columns": ["col1", "col2"],
    "creation_date": "2025-08-20T14:30:22",
    "encoding_type": "one_hot"
  }
}
```

### 7. ✅ Model Building & Evaluation
- **Multiple algorithm support**:
  - Logistic Regression
  - Random Forest Classifier
- **Comprehensive evaluation metrics**:
  - Accuracy, Precision, Recall, F1-Score
  - ROC AUC curves
  - Confusion matrices
- **Feature importance analysis**
- **Model comparison** and best model selection
- **Automated model saving** with metadata

**Model Artifacts:**
- Trained model files (.joblib)
- Model metadata (JSON)
- Performance reports
- Evaluation visualizations

### 8. ✅ Data Versioning
- **Automated dataset versioning** with timestamps
- **File integrity tracking** using MD5 hashes
- **Version metadata** including size, source, and changes
- **Version comparison** capabilities
- **Rollback support** to previous versions

**Version Structure:**
```
pipeline_data/versions/
├── v_20250820_143022/
│   ├── cleaned_churn_data.csv
│   └── metadata.json
└── v_20250820_144515/
    ├── transformed_churn_data.csv
    └── metadata.json
```

### 9. ✅ Pipeline Orchestration
- **Complete workflow automation** with dependency management
- **Stage-by-stage execution** with error handling
- **Comprehensive logging** for each pipeline step
- **Quality gates** with configurable thresholds
- **Automated rollback** on critical failures
- **Execution metrics** and performance monitoring

**Pipeline Stages:**
1. Data Ingestion
2. Data Validation  
3. Data Preparation
4. Feature Engineering
5. Model Training

### 10. ✅ Monitoring & Alerting
- **Pipeline execution tracking** with metrics
- **Data quality monitoring** over time
- **Model performance tracking**
- **System resource monitoring** (disk space, etc.)
- **Automated alerting** for failures and anomalies
- **Dashboard generation** for operational visibility

## Quick Start Guide

### 1. Environment Setup
```bash
# Install dependencies
pip install pandas numpy scikit-learn matplotlib seaborn joblib

# Install ingestion dependencies
pip install -r data_ingestion/requirements.txt

# Setup project structure
python -c "
import os
folders = ['pipeline_data/raw', 'pipeline_data/processed', 'pipeline_data/transformed', 
          'pipeline_data/models', 'pipeline_data/reports', 'pipeline_data/logs',
          'pipeline_data/feature_store', 'pipeline_data/versions', 'pipeline_data/monitoring']
for folder in folders:
    os.makedirs(folder, exist_ok=True)
print('✅ Project structure created')
"
```

### 2. Run Complete Pipeline
```python
# Option 1: Run in Jupyter Notebook
# Open and execute CHURN.ipynb

# Option 2: Run programmatically
from pipeline_orchestrator import ChurnDataPipelineOrchestrator

PROJECT_ROOT = "/path/to/your/project"
orchestrator = ChurnDataPipelineOrchestrator(PROJECT_ROOT)
success = orchestrator.execute_full_pipeline()
```

### 3. Monitor Pipeline
```bash
# Check ingestion status
python -m data_ingestion.main status

# View execution logs
ls pipeline_data/logs/

# Check data versions
ls pipeline_data/versions/

# View monitoring dashboard
ls pipeline_data/monitoring/
```

## Configuration

### Data Sources
Edit `data_ingestion/config.py` to configure your data sources:

```python
DATA_SOURCES = {
    "your_data_source": {
        "local_path": "/path/to/your/data",
        "file_pattern": "*.csv",
        "frequency": "daily",
        "enabled": True
    }
}
```

### Quality Thresholds
Adjust quality gates in `pipeline_orchestrator.py`:

```python
'quality_thresholds': {
    'min_quality_score': 70,
    'max_missing_percentage': 30,
    'max_duplicate_percentage': 10
}
```

### Model Thresholds
Set minimum model performance requirements:

```python
'model_thresholds': {
    'min_accuracy': 0.7,
    'min_roc_auc': 0.65
}
```

## Outputs & Deliverables

### 📊 Data Quality Reports
- `data_quality_report_YYYYMMDD_HHMMSS.json` - Comprehensive data validation results
- `statistical_summary_YYYYMMDD_HHMMSS.json` - Dataset statistical analysis

### 🔧 Processed Datasets
- `cleaned_churn_data_YYYYMMDD_HHMMSS.csv` - Cleaned and validated data
- `transformed_churn_data_YYYYMMDD_HHMMSS.csv` - Feature-engineered dataset

### 🤖 Trained Models
- `best_churn_model_YYYYMMDD_HHMMSS.joblib` - Best performing model
- `model_metadata_YYYYMMDD_HHMMSS.json` - Model performance and metadata

### 📈 Visualizations
- `eda_analysis_YYYYMMDD_HHMMSS.png` - Exploratory data analysis plots
- `correlation_heatmap_YYYYMMDD_HHMMSS.png` - Feature correlation matrix
- `model_evaluation_report_YYYYMMDD_HHMMSS.png` - Model performance visualizations

### 📋 Pipeline Reports
- `pipeline_execution_report_YYYYMMDD_HHMMSS.json` - Complete pipeline execution summary
- `preparation_log_YYYYMMDD_HHMMSS.json` - Data preparation steps log

### 🔍 Monitoring
- `monitoring_dashboard_YYYYMMDD_HHMMSS.json` - Pipeline health dashboard
- `ingestion_status.json` - Data ingestion status tracking

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Install missing dependencies
   pip install pandas numpy scikit-learn matplotlib seaborn joblib schedule requests
   ```

2. **Permission Errors**
   ```bash
   # Ensure write permissions for pipeline_data directory
   chmod -R 755 pipeline_data/
   ```

3. **Memory Issues with Large Datasets**
   ```python
   # Process data in chunks
   chunk_size = 10000
   for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
       process_chunk(chunk)
   ```

4. **Pipeline Stage Failures**
   - Check logs in `pipeline_data/logs/`
   - Review error messages in console output
   - Verify data quality thresholds
   - Ensure sufficient disk space

### Debug Mode
Enable detailed logging:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## Future Enhancements

- [ ] **Real-time streaming** data ingestion
- [ ] **Advanced ML algorithms** (XGBoost, Neural Networks)
- [ ] **Hyperparameter optimization** using Optuna
- [ ] **Model drift detection** and automated retraining
- [ ] **API endpoints** for model serving
- [ ] **Docker containerization** for deployment
- [ ] **Cloud deployment** (AWS/Azure/GCP)
- [ ] **Advanced monitoring** with Prometheus/Grafana

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review log files in `pipeline_data/logs/`
3. Validate your configuration settings
4. Ensure all dependencies are installed

## License

This project is for educational and demonstration purposes.

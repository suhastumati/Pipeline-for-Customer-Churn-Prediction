"""
Complete Data Pipeline Orchestrator
This module orchestrates the entire data pipeline from ingestion to model deployment
"""
import os
import json
import time
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
import logging

class ChurnDataPipelineOrchestrator:
    def __init__(self, project_root: str):
        self.project_root = project_root
        self.pipeline_config = self._load_pipeline_config()
        self.pipeline_state = {}
        self.execution_log = []
        
        # Setup logging
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging for pipeline"""
        log_dir = os.path.join(self.project_root, "pipeline_data", "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = f"pipeline_orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = os.path.join(log_dir, log_filename)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath),
                logging.StreamHandler()
            ]
        )
        
        return logging.getLogger('ChurnPipelineOrchestrator')
    
    def _load_pipeline_config(self) -> Dict[str, Any]:
        """Load pipeline configuration"""
        return {
            'data_source': os.path.join(self.project_root, "churn_dataset", "cell2cellholdout.csv"),
            'folders': {
                'raw_data': os.path.join(self.project_root, "pipeline_data", "raw"),
                'processed_data': os.path.join(self.project_root, "pipeline_data", "processed"),
                'transformed_data': os.path.join(self.project_root, "pipeline_data", "transformed"),
                'models': os.path.join(self.project_root, "pipeline_data", "models"),
                'reports': os.path.join(self.project_root, "pipeline_data", "reports"),
                'logs': os.path.join(self.project_root, "pipeline_data", "logs"),
                'feature_store': os.path.join(self.project_root, "pipeline_data", "feature_store")
            },
            'quality_thresholds': {
                'min_quality_score': 70,
                'max_missing_percentage': 30,
                'max_duplicate_percentage': 10
            },
            'model_thresholds': {
                'min_accuracy': 0.7,
                'min_roc_auc': 0.65
            }
        }
    
    def _create_directories(self):
        """Create all necessary directories"""
        self.logger.info("Creating project directories...")
        
        for folder_name, folder_path in self.pipeline_config['folders'].items():
            os.makedirs(folder_path, exist_ok=True)
            self.logger.info(f"Created directory: {folder_name}")
    
    def _log_stage_start(self, stage_name: str):
        """Log the start of a pipeline stage"""
        timestamp = datetime.now().isoformat()
        self.logger.info(f"=== STARTING STAGE: {stage_name} ===")
        
        stage_info = {
            'stage': stage_name,
            'start_time': timestamp,
            'status': 'started'
        }
        
        self.execution_log.append(stage_info)
        return len(self.execution_log) - 1  # Return index for updating
    
    def _log_stage_completion(self, stage_index: int, success: bool, metrics: Dict = None):
        """Log the completion of a pipeline stage"""
        timestamp = datetime.now().isoformat()
        stage_info = self.execution_log[stage_index]
        
        stage_info.update({
            'end_time': timestamp,
            'status': 'success' if success else 'failed',
            'duration': self._calculate_duration(stage_info['start_time'], timestamp),
            'metrics': metrics or {}
        })
        
        status_msg = "COMPLETED" if success else "FAILED"
        self.logger.info(f"=== {status_msg} STAGE: {stage_info['stage']} ===")
    
    def _calculate_duration(self, start_time: str, end_time: str) -> float:
        """Calculate duration between two timestamps"""
        start = datetime.fromisoformat(start_time)
        end = datetime.fromisoformat(end_time)
        return (end - start).total_seconds()
    
    def stage_1_data_ingestion(self) -> bool:
        """Stage 1: Data Ingestion with Error Handling"""
        stage_idx = self._log_stage_start("Data Ingestion")
        
        try:
            from data_ingestion.orchestrator import orchestrator
            
            # Configure ingestion for churn data
            churn_config = {
                'local_path': os.path.dirname(self.pipeline_config['data_source']),
                'file_pattern': '*.csv',
                'frequency': 'manual',
                'enabled': True
            }
            
            # Execute ingestion
            success = orchestrator.ingest_single_source('churn_data', churn_config)
            
            if success:
                # Verify data was ingested
                data_files = [f for f in os.listdir(self.pipeline_config['folders']['raw_data']) 
                             if f.endswith('.csv')]
                
                metrics = {
                    'files_ingested': len(data_files),
                    'ingestion_method': 'local_file_copy'
                }
                
                self._log_stage_completion(stage_idx, True, metrics)
                return True
            else:
                self._log_stage_completion(stage_idx, False)
                return False
                
        except Exception as e:
            self.logger.error(f"Data ingestion failed: {str(e)}")
            self._log_stage_completion(stage_idx, False)
            return False
    
    def stage_2_data_validation(self) -> bool:
        """Stage 2: Data Validation and Quality Checks"""
        stage_idx = self._log_stage_start("Data Validation")
        
        try:
            import pandas as pd
            
            # Load raw data
            df = pd.read_csv(self.pipeline_config['data_source'])
            
            # Run validation checks
            missing_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            duplicate_percentage = (df.duplicated().sum() / len(df)) * 100
            
            # Calculate quality score
            quality_score = 100 - (missing_percentage * 0.5) - (duplicate_percentage * 0.8)
            
            # Check thresholds
            quality_checks = {
                'quality_score': quality_score >= self.pipeline_config['quality_thresholds']['min_quality_score'],
                'missing_data': missing_percentage <= self.pipeline_config['quality_thresholds']['max_missing_percentage'],
                'duplicates': duplicate_percentage <= self.pipeline_config['quality_thresholds']['max_duplicate_percentage']
            }
            
            all_passed = all(quality_checks.values())
            
            metrics = {
                'quality_score': quality_score,
                'missing_percentage': missing_percentage,
                'duplicate_percentage': duplicate_percentage,
                'checks_passed': quality_checks
            }
            
            self._log_stage_completion(stage_idx, all_passed, metrics)
            
            if not all_passed:
                self.logger.warning("Data quality checks failed but proceeding with pipeline")
            
            return True  # Continue pipeline even if quality is suboptimal
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            self._log_stage_completion(stage_idx, False)
            return False
    
    def stage_3_data_preparation(self) -> bool:
        """Stage 3: Data Preparation and Cleaning"""
        stage_idx = self._log_stage_start("Data Preparation")
        
        try:
            import pandas as pd
            import numpy as np
            
            # Load and prepare data
            df = pd.read_csv(self.pipeline_config['data_source'])
            original_shape = df.shape
            
            # Basic cleaning
            df = df.drop_duplicates()
            
            # Handle missing values
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            categorical_cols = df.select_dtypes(include=['object']).columns
            
            for col in numeric_cols:
                df[col] = df[col].fillna(df[col].median())
            
            for col in categorical_cols:
                df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
            
            # Save cleaned data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            cleaned_file = os.path.join(
                self.pipeline_config['folders']['processed_data'], 
                f"cleaned_churn_data_{timestamp}.csv"
            )
            df.to_csv(cleaned_file, index=False)
            
            metrics = {
                'original_shape': original_shape,
                'cleaned_shape': df.shape,
                'rows_removed': original_shape[0] - df.shape[0],
                'cleaned_file': cleaned_file
            }
            
            self.pipeline_state['cleaned_data_path'] = cleaned_file
            self._log_stage_completion(stage_idx, True, metrics)
            return True
            
        except Exception as e:
            self.logger.error(f"Data preparation failed: {str(e)}")
            self._log_stage_completion(stage_idx, False)
            return False
    
    def stage_4_feature_engineering(self) -> bool:
        """Stage 4: Feature Engineering and Transformation"""
        stage_idx = self._log_stage_start("Feature Engineering")
        
        try:
            import pandas as pd
            import numpy as np
            from sklearn.preprocessing import StandardScaler, LabelEncoder
            import joblib
            
            # Load cleaned data
            df = pd.read_csv(self.pipeline_state['cleaned_data_path'])
            original_features = len(df.columns)
            
            # Identify or create target variable
            churn_candidates = [col for col in df.columns if 'churn' in col.lower()]
            if churn_candidates:
                target_col = churn_candidates[0]
            else:
                # Create synthetic target for demonstration
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    threshold = df[numeric_cols[0]].quantile(0.7)
                    df['churn_flag'] = (df[numeric_cols[0]] > threshold).astype(int)
                    target_col = 'churn_flag'
                else:
                    raise ValueError("Cannot create target variable")
            
            # Feature engineering
            numeric_features = [col for col in df.select_dtypes(include=[np.number]).columns if col != target_col]
            categorical_features = [col for col in df.select_dtypes(include=['object']).columns if col != target_col]
            
            # Create aggregated features
            if len(numeric_features) >= 2:
                df['feature_sum'] = df[numeric_features[:3]].sum(axis=1)
                df['feature_mean'] = df[numeric_features[:3]].mean(axis=1)
            
            # Encode categorical variables
            encoders = {}
            for col in categorical_features:
                if df[col].nunique() <= 10:
                    # One-hot encoding
                    dummies = pd.get_dummies(df[col], prefix=col, drop_first=True)
                    df = pd.concat([df, dummies], axis=1)
                    df = df.drop(col, axis=1)
                else:
                    # Label encoding
                    le = LabelEncoder()
                    df[f'{col}_encoded'] = le.fit_transform(df[col])
                    df = df.drop(col, axis=1)
                    encoders[col] = le
            
            # Scale numeric features
            scaler = StandardScaler()
            feature_cols = [col for col in df.columns if col != target_col and df[col].nunique() > 2]
            
            if feature_cols:
                df[feature_cols] = scaler.fit_transform(df[feature_cols])
            
            # Save transformed data and artifacts
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save transformed dataset
            transformed_file = os.path.join(
                self.pipeline_config['folders']['transformed_data'],
                f"transformed_churn_data_{timestamp}.csv"
            )
            df.to_csv(transformed_file, index=False)
            
            # Save encoders and scaler
            if encoders:
                encoders_file = os.path.join(
                    self.pipeline_config['folders']['feature_store'],
                    f"encoders_{timestamp}.joblib"
                )
                joblib.dump(encoders, encoders_file)
            
            scaler_file = os.path.join(
                self.pipeline_config['folders']['feature_store'],
                f"scaler_{timestamp}.joblib"
            )
            joblib.dump(scaler, scaler_file)
            
            metrics = {
                'original_features': original_features,
                'final_features': len(df.columns),
                'target_column': target_col,
                'features_created': len(df.columns) - original_features,
                'transformed_file': transformed_file
            }
            
            self.pipeline_state.update({
                'transformed_data_path': transformed_file,
                'target_column': target_col,
                'scaler_path': scaler_file,
                'encoders_path': encoders_file if encoders else None
            })
            
            self._log_stage_completion(stage_idx, True, metrics)
            return True
            
        except Exception as e:
            self.logger.error(f"Feature engineering failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            self._log_stage_completion(stage_idx, False)
            return False
    
    def stage_5_model_training(self) -> bool:
        """Stage 5: Model Training and Evaluation"""
        stage_idx = self._log_stage_start("Model Training")
        
        try:
            import pandas as pd
            import numpy as np
            from sklearn.model_selection import train_test_split
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.linear_model import LogisticRegression
            from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
            import joblib
            
            # Load transformed data
            df = pd.read_csv(self.pipeline_state['transformed_data_path'])
            target_col = self.pipeline_state['target_column']
            
            # Prepare data for modeling
            X = df.drop(columns=[target_col])
            y = df[target_col]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, 
                stratify=y if len(y.unique()) > 1 else None
            )
            
            # Train models
            models = {
                'random_forest': RandomForestClassifier(random_state=42, n_estimators=100),
                'logistic_regression': LogisticRegression(random_state=42, max_iter=1000)
            }
            
            model_results = {}
            best_model = None
            best_score = 0
            
            for name, model in models.items():
                try:
                    # Train model
                    model.fit(X_train, y_train)
                    
                    # Evaluate model
                    y_pred = model.predict(X_test)
                    accuracy = accuracy_score(y_test, y_pred)
                    
                    # ROC AUC for binary classification
                    roc_auc = None
                    if len(np.unique(y)) == 2 and hasattr(model, 'predict_proba'):
                        y_pred_proba = model.predict_proba(X_test)[:, 1]
                        roc_auc = roc_auc_score(y_test, y_pred_proba)
                    
                    model_results[name] = {
                        'accuracy': accuracy,
                        'roc_auc': roc_auc,
                        'model': model
                    }
                    
                    # Track best model
                    if accuracy > best_score:
                        best_score = accuracy
                        best_model = (name, model)
                    
                    self.logger.info(f"{name} - Accuracy: {accuracy:.4f}, ROC AUC: {roc_auc:.4f if roc_auc else 'N/A'}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to train {name}: {str(e)}")
            
            # Save best model
            if best_model:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                model_file = os.path.join(
                    self.pipeline_config['folders']['models'],
                    f"best_churn_model_{timestamp}.joblib"
                )
                
                joblib.dump(best_model[1], model_file)
                
                # Save model metadata
                model_metadata = {
                    'model_name': best_model[0],
                    'accuracy': model_results[best_model[0]]['accuracy'],
                    'roc_auc': model_results[best_model[0]]['roc_auc'],
                    'training_timestamp': datetime.now().isoformat(),
                    'model_file': model_file,
                    'features': X.columns.tolist(),
                    'target': target_col
                }
                
                metadata_file = os.path.join(
                    self.pipeline_config['folders']['models'],
                    f"model_metadata_{timestamp}.json"
                )
                
                with open(metadata_file, 'w') as f:
                    json.dump(model_metadata, f, indent=2, default=str)
                
                # Check if model meets thresholds
                meets_accuracy = model_results[best_model[0]]['accuracy'] >= self.pipeline_config['model_thresholds']['min_accuracy']
                meets_roc_auc = (model_results[best_model[0]]['roc_auc'] or 0) >= self.pipeline_config['model_thresholds']['min_roc_auc']
                
                metrics = {
                    'models_trained': len(model_results),
                    'best_model': best_model[0],
                    'best_accuracy': model_results[best_model[0]]['accuracy'],
                    'best_roc_auc': model_results[best_model[0]]['roc_auc'],
                    'meets_thresholds': meets_accuracy and meets_roc_auc,
                    'model_file': model_file
                }
                
                self.pipeline_state['best_model_path'] = model_file
                self.pipeline_state['model_metadata'] = model_metadata
                
                self._log_stage_completion(stage_idx, True, metrics)
                return True
            else:
                self._log_stage_completion(stage_idx, False)
                return False
                
        except Exception as e:
            self.logger.error(f"Model training failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            self._log_stage_completion(stage_idx, False)
            return False
    
    def generate_pipeline_report(self) -> str:
        """Generate comprehensive pipeline execution report"""
        self.logger.info("Generating pipeline execution report...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(
            self.pipeline_config['folders']['reports'],
            f"pipeline_execution_report_{timestamp}.json"
        )
        
        # Calculate overall pipeline metrics
        successful_stages = sum(1 for stage in self.execution_log if stage['status'] == 'success')
        total_stages = len(self.execution_log)
        total_duration = sum(stage.get('duration', 0) for stage in self.execution_log)
        
        pipeline_report = {
            'pipeline_metadata': {
                'execution_timestamp': datetime.now().isoformat(),
                'total_stages': total_stages,
                'successful_stages': successful_stages,
                'success_rate': (successful_stages / total_stages * 100) if total_stages > 0 else 0,
                'total_duration_seconds': total_duration,
                'pipeline_status': 'SUCCESS' if successful_stages == total_stages else 'PARTIAL_FAILURE'
            },
            'stage_execution_log': self.execution_log,
            'pipeline_state': self.pipeline_state,
            'data_lineage': {
                'source_data': self.pipeline_config['data_source'],
                'intermediate_outputs': [
                    self.pipeline_state.get('cleaned_data_path'),
                    self.pipeline_state.get('transformed_data_path')
                ],
                'final_model': self.pipeline_state.get('best_model_path')
            },
            'quality_metrics': self._extract_quality_metrics(),
            'recommendations': self._generate_recommendations()
        }
        
        with open(report_file, 'w') as f:
            json.dump(pipeline_report, f, indent=2, default=str)
        
        self.logger.info(f"Pipeline report saved to: {report_file}")
        return report_file
    
    def _extract_quality_metrics(self) -> Dict[str, Any]:
        """Extract quality metrics from execution log"""
        quality_metrics = {}
        
        for stage in self.execution_log:
            if stage.get('metrics'):
                quality_metrics[stage['stage']] = stage['metrics']
        
        return quality_metrics
    
    def _generate_recommendations(self) -> list:
        """Generate recommendations based on pipeline execution"""
        recommendations = []
        
        # Check for failed stages
        failed_stages = [stage for stage in self.execution_log if stage['status'] == 'failed']
        if failed_stages:
            recommendations.append(f"Review and fix {len(failed_stages)} failed pipeline stages")
        
        # Check data quality
        validation_stage = next((stage for stage in self.execution_log if stage['stage'] == 'Data Validation'), None)
        if validation_stage and validation_stage.get('metrics'):
            quality_score = validation_stage['metrics'].get('quality_score', 0)
            if quality_score < 80:
                recommendations.append("Improve data quality before production deployment")
        
        # Check model performance
        model_stage = next((stage for stage in self.execution_log if stage['stage'] == 'Model Training'), None)
        if model_stage and model_stage.get('metrics'):
            accuracy = model_stage['metrics'].get('best_accuracy', 0)
            if accuracy < 0.8:
                recommendations.append("Consider feature engineering or hyperparameter tuning to improve model performance")
        
        if not recommendations:
            recommendations.append("Pipeline executed successfully - ready for production deployment")
        
        return recommendations
    
    def execute_full_pipeline(self) -> bool:
        """Execute the complete data pipeline"""
        self.logger.info("Starting complete data pipeline execution...")
        start_time = datetime.now()
        
        # Create directories
        self._create_directories()
        
        # Execute pipeline stages
        pipeline_steps = [
            ("Data Ingestion", self.stage_1_data_ingestion),
            ("Data Validation", self.stage_2_data_validation),
            ("Data Preparation", self.stage_3_data_preparation),
            ("Feature Engineering", self.stage_4_feature_engineering),
            ("Model Training", self.stage_5_model_training)
        ]
        
        for step_name, step_function in pipeline_steps:
            try:
                success = step_function()
                if not success:
                    self.logger.error(f"Pipeline failed at stage: {step_name}")
                    break
            except Exception as e:
                self.logger.error(f"Unexpected error in {step_name}: {str(e)}")
                self.logger.error(traceback.format_exc())
                break
        
        # Generate final report
        report_file = self.generate_pipeline_report()
        
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        successful_stages = sum(1 for stage in self.execution_log if stage['status'] == 'success')
        total_stages = len(self.execution_log)
        
        self.logger.info(f"Pipeline execution completed in {total_duration:.2f} seconds")
        self.logger.info(f"Stages completed successfully: {successful_stages}/{total_stages}")
        self.logger.info(f"Pipeline report: {report_file}")
        
        return successful_stages == total_stages

if __name__ == "__main__":
    PROJECT_ROOT = "/Users/I528946/Desktop/Use cases/use case 1/AiImageDetection"
    
    orchestrator = ChurnDataPipelineOrchestrator(PROJECT_ROOT)
    success = orchestrator.execute_full_pipeline()
    
    if success:
        print("✓ Complete data pipeline executed successfully!")
    else:
        print("✗ Pipeline execution completed with errors. Check logs for details.")

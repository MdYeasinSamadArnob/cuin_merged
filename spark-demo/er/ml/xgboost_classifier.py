"""
XGBoost Entity Classifier

This module implements an XGBoost-based classifier for entity resolution.
It takes similarity features from entity pairs and classifies them as
duplicates or non-duplicates using machine learning.
"""

import json
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

try:
    import xgboost as xgb
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import (
        classification_report, confusion_matrix, accuracy_score,
        roc_curve, auc, precision_recall_curve, f1_score
    )
except ImportError as e:
    raise ImportError(
        "XGBoost and scikit-learn are required. Install with: pip install xgboost scikit-learn numpy"
    ) from e

from er.utils.entity_resolution import (
    compute_similarity,
    levenshtein_similarity,
    name_similarity,
    phone_similarity,
    email_similarity,
    token_jaccard_similarity
)


class XGBoostEntityClassifier:
    """
    XGBoost-based entity resolution classifier.
    
    This classifier extracts similarity features between entity pairs and
    uses XGBoost to predict whether they are duplicates.
    
    Features extracted:
    - Name similarity (Levenshtein, token-based, soundex)
    - Email similarity
    - Phone similarity
    - Address similarity
    - DOB exact match
    - Overall weighted similarity
    """
    
    def __init__(self,
                 similarity_threshold: float = 0.75,
                 xgb_params: Optional[Dict[str, Any]] = None):
        """
        Initialize XGBoost entity classifier.
        
        Args:
            similarity_threshold: Base similarity threshold for comparison
            xgb_params: Optional XGBoost parameters
        """
        self.similarity_threshold = similarity_threshold
        
        # Default XGBoost parameters
        if xgb_params is None:
            xgb_params = {
                'objective': 'binary:logistic',
                'eval_metric': 'logloss',
                'max_depth': 6,
                'learning_rate': 0.1,
                'n_estimators': 100,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42
            }
        
        self.xgb_params = xgb_params
        self.model = None
        self.feature_names = [
            'name_similarity',
            'email_similarity', 
            'phone_similarity',
            'address_similarity',
            'dob_match',
            'overall_similarity'
        ]
        
        # Optimal threshold
        self.optimal_threshold = 0.5  # Default threshold
        self.threshold_method = 'default'  # Method used to find threshold
        
        # Statistics
        self.stats = {
            'total_pairs': 0,
            'predicted_duplicates': 0,
            'training_accuracy': 0.0,
            'test_accuracy': 0.0,
            'optimal_threshold': 0.5,
            'threshold_method': 'default'
        }
    
    def extract_features(self, record1: dict, record2: dict) -> List[float]:
        """
        Extract similarity features from a pair of records.
        
        Args:
            record1: First record
            record2: Second record
            
        Returns:
            List of feature values
        """
        features = []
        
        # 1. Name similarity
        name1 = str(record1.get('CUSNMF', '') or record1.get('name', '')).strip()
        name2 = str(record2.get('CUSNMF', '') or record2.get('name', '')).strip()
        if name1 and name2:
            features.append(name_similarity(name1, name2))
        else:
            features.append(0.0)
        
        # 2. Email similarity
        email1 = str(record1.get('MAILID', '') or record1.get('email', '')).strip()
        email2 = str(record2.get('MAILID', '') or record2.get('email', '')).strip()
        if email1 and email2:
            features.append(email_similarity(email1, email2))
        else:
            features.append(0.0)
        
        # 3. Phone similarity
        phone1 = str(record1.get('TELENO', '') or record1.get('phone', '')).strip()
        phone2 = str(record2.get('TELENO', '') or record2.get('phone', '')).strip()
        if phone1 and phone2:
            features.append(phone_similarity(phone1, phone2))
        else:
            features.append(0.0)
        
        # 4. Address similarity
        addr1 = str(record1.get('ADDRS1', '') or record1.get('address', '')).strip()
        addr2 = str(record2.get('ADDRS1', '') or record2.get('address', '')).strip()
        if addr1 and addr2:
            features.append(token_jaccard_similarity(addr1, addr2))
        else:
            features.append(0.0)
        
        # 5. DOB exact match
        dob1 = str(record1.get('CUSDOB', '') or record1.get('dob', '')).strip()
        dob2 = str(record2.get('CUSDOB', '') or record2.get('dob', '')).strip()
        if dob1 and dob2:
            features.append(1.0 if dob1 == dob2 else 0.0)
        else:
            features.append(0.0)
        
        # 6. Overall weighted similarity
        overall = compute_similarity(record1, record2)
        features.append(overall)
        
        return features
    
    def prepare_training_data(self, records: List[dict], 
                            candidate_pairs: List[Tuple[int, int]],
                            labels: List[int]) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare training data from records and labeled pairs.
        
        Args:
            records: List of record dictionaries
            candidate_pairs: List of (idx1, idx2) tuples
            labels: List of labels (1 for duplicate, 0 for non-duplicate)
            
        Returns:
            Tuple of (features DataFrame, labels Series)
        """
        X = []
        for idx1, idx2 in candidate_pairs:
            features = self.extract_features(records[idx1], records[idx2])
            X.append(features)
        
        X_df = pd.DataFrame(X, columns=self.feature_names)
        y = pd.Series(labels)
        
        return X_df, y
    
    def train(self, X_train: pd.DataFrame, y_train: pd.Series,
              X_test: Optional[pd.DataFrame] = None,
              y_test: Optional[pd.Series] = None) -> Dict[str, Any]:
        """
        Train the XGBoost classifier.
        
        Args:
            X_train: Training features
            y_train: Training labels
            X_test: Optional test features
            y_test: Optional test labels
            
        Returns:
            Dictionary with training results
        """
        print(f"Training XGBoost classifier with {len(X_train)} samples...")
        
        # Check for class imbalance
        pos_count = sum(y_train)
        neg_count = len(y_train) - pos_count
        
        if pos_count == 0:
            print("  ⚠️  No positive samples found. Cannot train classifier.")
            print("     Try lowering the similarity threshold for label generation.")
            return {
                'train_accuracy': 0.0,
                'train_samples': len(X_train),
                'feature_importance': [{'feature': f, 'importance': 0.0} for f in self.feature_names],
                'error': 'No positive samples'
            }
        
        # Adjust scale_pos_weight for imbalanced data
        scale_pos_weight = neg_count / pos_count if pos_count > 0 else 1.0
        xgb_params = self.xgb_params.copy()
        if 'scale_pos_weight' not in xgb_params:
            xgb_params['scale_pos_weight'] = scale_pos_weight
        
        # Create XGBoost classifier
        self.model = xgb.XGBClassifier(**xgb_params)
        
        # Train the model
        eval_set = [(X_train, y_train)]
        if X_test is not None and y_test is not None:
            eval_set.append((X_test, y_test))
        
        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            verbose=False
        )
        
        # Evaluate on training set
        y_train_pred = self.model.predict(X_train)
        train_accuracy = accuracy_score(y_train, y_train_pred)
        self.stats['training_accuracy'] = train_accuracy
        
        results = {
            'train_accuracy': train_accuracy,
            'train_samples': len(X_train)
        }
        
        # Evaluate on test set if provided
        if X_test is not None and y_test is not None:
            y_test_pred = self.model.predict(X_test)
            test_accuracy = accuracy_score(y_test, y_test_pred)
            self.stats['test_accuracy'] = test_accuracy
            
            results['test_accuracy'] = test_accuracy
            results['test_samples'] = len(X_test)
            results['confusion_matrix'] = confusion_matrix(y_test, y_test_pred).tolist()
            results['classification_report'] = classification_report(y_test, y_test_pred)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        results['feature_importance'] = feature_importance.to_dict('records')
        
        print(f"  ✅ Training completed. Accuracy: {train_accuracy:.3f}")
        
        return results
    
    def find_optimal_threshold(self, X: pd.DataFrame, y: pd.Series, 
                              method: str = 'f1') -> Dict[str, Any]:
        """
        Find optimal classification threshold using various methods.
        
        Args:
            X: Feature DataFrame
            y: True labels
            method: Method to use for threshold optimization
                   - 'f1': Maximize F1 score (default)
                   - 'youden': Maximize Youden's J statistic (sensitivity + specificity - 1)
                   - 'precision_recall': Balance precision and recall at intersection
                   
        Returns:
            Dictionary with threshold information
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        # Get prediction probabilities
        y_proba = self.model.predict_proba(X)[:, 1]
        
        if method == 'f1':
            # Find threshold that maximizes F1 score
            thresholds = np.linspace(0.1, 0.9, 81)  # Test thresholds from 0.1 to 0.9
            f1_scores = []
            
            for threshold in thresholds:
                y_pred = (y_proba >= threshold).astype(int)
                # Use zero_division parameter to handle edge cases
                f1 = f1_score(y, y_pred, zero_division=0.0)
                f1_scores.append(f1)
            
            best_idx = np.argmax(f1_scores)
            optimal_threshold = thresholds[best_idx]
            best_f1 = f1_scores[best_idx]
            
            result = {
                'method': 'f1',
                'optimal_threshold': float(optimal_threshold),
                'f1_score': float(best_f1),
                'all_thresholds': thresholds.tolist(),
                'all_f1_scores': f1_scores
            }
            
        elif method == 'youden':
            # Find threshold that maximizes Youden's J statistic (TPR - FPR)
            fpr, tpr, thresholds = roc_curve(y, y_proba)
            
            # Youden's J statistic = sensitivity + specificity - 1 = TPR - FPR
            j_scores = tpr - fpr
            best_idx = np.argmax(j_scores)
            optimal_threshold = thresholds[best_idx]
            
            result = {
                'method': 'youden',
                'optimal_threshold': float(optimal_threshold),
                'youden_j': float(j_scores[best_idx]),
                'tpr': float(tpr[best_idx]),
                'fpr': float(fpr[best_idx]),
                'roc_auc': float(auc(fpr, tpr))
            }
            
        elif method == 'precision_recall':
            # Find threshold where precision and recall are balanced
            precision, recall, thresholds = precision_recall_curve(y, y_proba)
            
            # Find point closest to where precision = recall
            # or maximize F1 score (harmonic mean of precision and recall)
            f1_scores = 2 * (precision[:-1] * recall[:-1]) / (precision[:-1] + recall[:-1] + 1e-10)
            best_idx = np.argmax(f1_scores)
            optimal_threshold = thresholds[best_idx]
            
            result = {
                'method': 'precision_recall',
                'optimal_threshold': float(optimal_threshold),
                'precision': float(precision[:-1][best_idx]),
                'recall': float(recall[:-1][best_idx]),
                'f1_score': float(f1_scores[best_idx])
            }
            
        else:
            raise ValueError(f"Unknown method: {method}. Use 'f1', 'youden', or 'precision_recall'")
        
        # Store the optimal threshold
        self.optimal_threshold = result['optimal_threshold']
        self.threshold_method = method
        self.stats['optimal_threshold'] = self.optimal_threshold
        self.stats['threshold_method'] = method
        
        return result
    
    def compare_thresholds(self, X: pd.DataFrame, y: pd.Series, 
                          use_method: str = 'f1') -> Dict[str, Any]:
        """
        Compare different threshold optimization methods.
        
        Args:
            X: Feature DataFrame
            y: True labels
            use_method: Which method to use for setting the optimal threshold (default: 'f1')
            
        Returns:
            Dictionary with comparison results for all methods
        """
        methods = ['f1', 'youden', 'precision_recall']
        results = {}
        
        # Store original threshold settings
        original_threshold = self.optimal_threshold
        original_method = self.threshold_method
        
        # Calculate results for all methods
        for method in methods:
            try:
                result = self.find_optimal_threshold(X, y, method=method)
                results[method] = result
            except Exception as e:
                results[method] = {'error': str(e)}
        
        # Set the threshold to the chosen method
        if use_method in results and 'error' not in results[use_method]:
            self.optimal_threshold = results[use_method]['optimal_threshold']
            self.threshold_method = use_method
            self.stats['optimal_threshold'] = self.optimal_threshold
            self.stats['threshold_method'] = use_method
        else:
            # Restore original if chosen method failed
            self.optimal_threshold = original_threshold
            self.threshold_method = original_method
            self.stats['optimal_threshold'] = original_threshold
            self.stats['threshold_method'] = original_method
        
        return results
    
    def predict(self, records: List[dict], 
                candidate_pairs: List[Tuple[int, int]],
                use_optimal_threshold: bool = True) -> List[Dict[str, Any]]:
        """
        Predict duplicates using trained model.
        
        Args:
            records: List of record dictionaries
            candidate_pairs: List of (idx1, idx2) tuples to classify
            use_optimal_threshold: If True, use optimal threshold instead of default 0.5
            
        Returns:
            List of match results with predictions
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")
        
        # Extract features for all pairs
        X = []
        for idx1, idx2 in candidate_pairs:
            features = self.extract_features(records[idx1], records[idx2])
            X.append(features)
        
        X_df = pd.DataFrame(X, columns=self.feature_names)
        
        # Get prediction probabilities
        probabilities = self.model.predict_proba(X_df)[:, 1]
        
        # Use optimal threshold if available and requested
        threshold = self.optimal_threshold if use_optimal_threshold else 0.5
        predictions = (probabilities >= threshold).astype(int)
        
        # Prepare results
        results = []
        for i, (idx1, idx2) in enumerate(candidate_pairs):
            if predictions[i] == 1:
                results.append({
                    'record1_idx': idx1,
                    'record2_idx': idx2,
                    'record1': records[idx1],
                    'record2': records[idx2],
                    'prediction': 'duplicate',
                    'probability': float(probabilities[i]),
                    'threshold': float(threshold),
                    'features': dict(zip(self.feature_names, X[i]))
                })
        
        self.stats['total_pairs'] = len(candidate_pairs)
        self.stats['predicted_duplicates'] = len(results)
        
        return results
    
    def classify_and_cluster(self, records: List[dict],
                           candidate_pairs: List[Tuple[int, int]]) -> Dict[str, Any]:
        """
        Classify pairs and create clusters using transitive closure.
        
        Args:
            records: List of record dictionaries
            candidate_pairs: List of (idx1, idx2) tuples
            
        Returns:
            Dictionary with matches and clusters
        """
        matches = self.predict(records, candidate_pairs)
        
        # Create clusters using union-find
        parent = list(range(len(records)))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # Build clusters from matches
        for match in matches:
            union(match['record1_idx'], match['record2_idx'])
        
        # Group records by cluster
        clusters_map = defaultdict(list)
        for idx in range(len(records)):
            cluster_id = find(idx)
            clusters_map[cluster_id].append(idx)
        
        # Filter out singleton clusters
        clusters = [
            {
                'cluster_id': cluster_id,
                'size': len(members),
                'member_indices': members,
                'members': [records[i] for i in members]
            }
            for cluster_id, members in clusters_map.items()
            if len(members) > 1
        ]
        
        return {
            'matches': matches,
            'clusters': clusters,
            'stats': self.stats
        }
    
    def print_statistics(self):
        """Print classifier statistics."""
        print("\n" + "="*60)
        print("XGBoost Entity Classifier Statistics")
        print("="*60)
        print(f"Total pairs classified:    {self.stats['total_pairs']}")
        print(f"Predicted duplicates:      {self.stats['predicted_duplicates']}")
        print(f"Training accuracy:         {self.stats['training_accuracy']:.3f}")
        print(f"Test accuracy:             {self.stats['test_accuracy']:.3f}")
        print(f"Optimal threshold:         {self.stats['optimal_threshold']:.3f}")
        print(f"Threshold method:          {self.stats['threshold_method']}")
        print("="*60 + "\n")
    
    def save_model(self, filepath: str):
        """Save trained model to file."""
        if self.model is None:
            raise ValueError("No model to save. Train the model first.")
        self.model.save_model(filepath)
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load trained model from file."""
        self.model = xgb.XGBClassifier(**self.xgb_params)
        self.model.load_model(filepath)
        print(f"Model loaded from {filepath}")

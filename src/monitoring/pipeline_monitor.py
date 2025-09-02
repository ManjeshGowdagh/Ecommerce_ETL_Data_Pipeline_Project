"""
Pipeline monitoring and alerting module.
Tracks pipeline execution, performance, and data quality metrics.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict

from config.pipeline_config import CONFIG


@dataclass
class PipelineMetrics:
    """Data class for pipeline execution metrics."""
    pipeline_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "RUNNING"
    stages_completed: List[str] = None
    stages_failed: List[str] = None
    total_records_processed: int = 0
    data_quality_score: float = 0.0
    performance_metrics: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.stages_completed is None:
            self.stages_completed = []
        if self.stages_failed is None:
            self.stages_failed = []
        if self.performance_metrics is None:
            self.performance_metrics = {}


class PipelineMonitor:
    """Monitors pipeline execution and generates alerts."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics_file = f"{CONFIG.logs_path}/pipeline_metrics.json"
        self.alerts_file = f"{CONFIG.logs_path}/pipeline_alerts.json"
        
        # Ensure directories exist
        os.makedirs(CONFIG.logs_path, exist_ok=True)
    
    def start_pipeline_monitoring(self, pipeline_id: str) -> PipelineMetrics:
        """Start monitoring a new pipeline execution."""
        metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            start_time=datetime.now()
        )
        
        self.save_metrics(metrics)
        self.logger.info(f"Started monitoring pipeline: {pipeline_id}")
        
        return metrics
    
    def update_stage_completion(self, metrics: PipelineMetrics, stage_name: str, 
                              records_processed: int = 0) -> None:
        """Update metrics when a stage completes successfully."""
        metrics.stages_completed.append(stage_name)
        metrics.total_records_processed += records_processed
        
        self.save_metrics(metrics)
        self.logger.info(f"Stage completed: {stage_name} ({records_processed} records)")
    
    def update_stage_failure(self, metrics: PipelineMetrics, stage_name: str, 
                           error_message: str) -> None:
        """Update metrics when a stage fails."""
        metrics.stages_failed.append(stage_name)
        metrics.status = "FAILED"
        
        # Generate alert
        self.generate_alert("STAGE_FAILURE", {
            "pipeline_id": metrics.pipeline_id,
            "stage": stage_name,
            "error": error_message,
            "timestamp": datetime.now().isoformat()
        })
        
        self.save_metrics(metrics)
        self.logger.error(f"Stage failed: {stage_name} - {error_message}")
    
    def complete_pipeline_monitoring(self, metrics: PipelineMetrics, 
                                   data_quality_score: float = 0.0) -> None:
        """Complete pipeline monitoring and finalize metrics."""
        metrics.end_time = datetime.now()
        metrics.data_quality_score = data_quality_score
        
        if not metrics.stages_failed:
            metrics.status = "SUCCESS"
        
        # Calculate performance metrics
        duration = metrics.end_time - metrics.start_time
        metrics.performance_metrics = {
            "duration_seconds": duration.total_seconds(),
            "duration_formatted": str(duration),
            "records_per_second": metrics.total_records_processed / duration.total_seconds() if duration.total_seconds() > 0 else 0
        }
        
        # Check for performance alerts
        if duration.total_seconds() > 3600:  # More than 1 hour
            self.generate_alert("PERFORMANCE_WARNING", {
                "pipeline_id": metrics.pipeline_id,
                "duration": str(duration),
                "message": "Pipeline execution exceeded expected duration"
            })
        
        # Check for data quality alerts
        if data_quality_score < 90:
            self.generate_alert("DATA_QUALITY_WARNING", {
                "pipeline_id": metrics.pipeline_id,
                "quality_score": data_quality_score,
                "message": "Data quality score below acceptable threshold"
            })
        
        self.save_metrics(metrics)
        self.logger.info(f"Pipeline monitoring completed: {metrics.pipeline_id} - {metrics.status}")
    
    def save_metrics(self, metrics: PipelineMetrics) -> None:
        """Save pipeline metrics to file."""
        try:
            # Load existing metrics
            existing_metrics = []
            if os.path.exists(self.metrics_file):
                with open(self.metrics_file, 'r') as f:
                    existing_metrics = json.load(f)
            
            # Update or add current metrics
            metrics_dict = asdict(metrics)
            metrics_dict["start_time"] = metrics.start_time.isoformat()
            if metrics.end_time:
                metrics_dict["end_time"] = metrics.end_time.isoformat()
            
            # Find and update existing entry or add new one
            updated = False
            for i, existing in enumerate(existing_metrics):
                if existing["pipeline_id"] == metrics.pipeline_id:
                    existing_metrics[i] = metrics_dict
                    updated = True
                    break
            
            if not updated:
                existing_metrics.append(metrics_dict)
            
            # Save updated metrics
            with open(self.metrics_file, 'w') as f:
                json.dump(existing_metrics, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving metrics: {str(e)}")
    
    def generate_alert(self, alert_type: str, alert_data: Dict[str, Any]) -> None:
        """Generate and save pipeline alerts."""
        alert = {
            "alert_id": f"{alert_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "alert_type": alert_type,
            "timestamp": datetime.now().isoformat(),
            "data": alert_data
        }
        
        try:
            # Load existing alerts
            existing_alerts = []
            if os.path.exists(self.alerts_file):
                with open(self.alerts_file, 'r') as f:
                    existing_alerts = json.load(f)
            
            # Add new alert
            existing_alerts.append(alert)
            
            # Keep only last 100 alerts
            existing_alerts = existing_alerts[-100:]
            
            # Save alerts
            with open(self.alerts_file, 'w') as f:
                json.dump(existing_alerts, f, indent=2)
            
            self.logger.warning(f"Alert generated: {alert_type} - {alert_data}")
            
        except Exception as e:
            self.logger.error(f"Error saving alert: {str(e)}")
    
    def get_pipeline_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get pipeline execution history for the last N days."""
        try:
            if not os.path.exists(self.metrics_file):
                return []
            
            with open(self.metrics_file, 'r') as f:
                all_metrics = json.load(f)
            
            # Filter by date
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_metrics = []
            
            for metrics in all_metrics:
                start_time = datetime.fromisoformat(metrics["start_time"])
                if start_time >= cutoff_date:
                    recent_metrics.append(metrics)
            
            return recent_metrics
            
        except Exception as e:
            self.logger.error(f"Error retrieving pipeline history: {str(e)}")
            return []
    
    def generate_monitoring_report(self) -> Dict[str, Any]:
        """Generate comprehensive monitoring report."""
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "pipeline_history": self.get_pipeline_history(7),
            "recent_alerts": self.get_recent_alerts(24),
            "performance_summary": self.calculate_performance_summary()
        }
        
        return report
    
    def get_recent_alerts(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get alerts from the last N hours."""
        try:
            if not os.path.exists(self.alerts_file):
                return []
            
            with open(self.alerts_file, 'r') as f:
                all_alerts = json.load(f)
            
            # Filter by time
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_alerts = []
            
            for alert in all_alerts:
                alert_time = datetime.fromisoformat(alert["timestamp"])
                if alert_time >= cutoff_time:
                    recent_alerts.append(alert)
            
            return recent_alerts
            
        except Exception as e:
            self.logger.error(f"Error retrieving recent alerts: {str(e)}")
            return []
    
    def calculate_performance_summary(self) -> Dict[str, Any]:
        """Calculate performance summary from recent executions."""
        history = self.get_pipeline_history(7)
        
        if not history:
            return {"message": "No recent pipeline executions found"}
        
        successful_runs = [h for h in history if h["status"] == "SUCCESS"]
        failed_runs = [h for h in history if h["status"] == "FAILED"]
        
        summary = {
            "total_executions": len(history),
            "successful_executions": len(successful_runs),
            "failed_executions": len(failed_runs),
            "success_rate": round((len(successful_runs) / len(history)) * 100, 2) if history else 0
        }
        
        if successful_runs:
            durations = [
                h["performance_metrics"]["duration_seconds"] 
                for h in successful_runs 
                if h.get("performance_metrics", {}).get("duration_seconds")
            ]
            
            if durations:
                summary["avg_duration_seconds"] = round(sum(durations) / len(durations), 2)
                summary["min_duration_seconds"] = min(durations)
                summary["max_duration_seconds"] = max(durations)
        
        return summary


def main():
    """Main monitoring execution for testing."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize monitor
    monitor = PipelineMonitor()
    
    # Generate monitoring report
    report = monitor.generate_monitoring_report()
    
    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{CONFIG.reports_path}/monitoring_report_{timestamp}.json"
    
    os.makedirs(CONFIG.reports_path, exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logging.info(f"Monitoring report generated: {report_path}")


if __name__ == "__main__":
    main()
"""
Additional utilities for ECS re-roll operations
Includes health checks, rollback capabilities, and advanced monitoring
"""

import boto3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import concurrent.futures
from botocore.exceptions import ClientError


@dataclass
class HealthCheckConfig:
    """Configuration for health checks"""
    alb_target_group_arns: List[str] = None
    cloudwatch_alarms: List[str] = None
    min_healthy_targets_percentage: int = 80
    health_check_interval_seconds: int = 30
    health_check_timeout_minutes: int = 10


class ECSHealthChecker:
    """Performs comprehensive health checks for ECS services"""
    
    def __init__(self, region: str = 'us-east-1'):
        self.region = region
        self.ecs_client = boto3.client('ecs', region_name=region)
        self.elb_client = boto3.client('elbv2', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    
    def check_service_health(self, cluster_name: str, service_name: str) -> Dict:
        """Check health of a specific ECS service"""
        try:
            response = self.ecs_client.describe_services(
                cluster=cluster_name,
                services=[service_name]
            )
            
            if not response['services']:
                return {'healthy': False, 'error': 'Service not found'}
            
            service = response['services'][0]
            
            # Check if service is stable
            is_healthy = (
                service['runningCount'] == service['desiredCount'] and
                service['pendingCount'] == 0 and
                service['status'] == 'ACTIVE'
            )
            
            return {
                'healthy': is_healthy,
                'service_name': service['serviceName'],
                'desired_count': service['desiredCount'],
                'running_count': service['runningCount'],
                'pending_count': service['pendingCount'],
                'status': service['status'],
                'deployments': len(service['deployments']),
                'events': service['events'][:5] if 'events' in service else []
            }
            
        except ClientError as e:
            return {'healthy': False, 'error': str(e)}
    
    def check_cluster_health(self, cluster_name: str) -> Dict:
        """Check overall cluster health"""
        try:
            # Get cluster info
            cluster_response = self.ecs_client.describe_clusters(
                clusters=[cluster_name]
            )
            
            if not cluster_response['clusters']:
                return {'healthy': False, 'error': 'Cluster not found'}
            
            cluster = cluster_response['clusters'][0]
            
            # List all services
            services_response = self.ecs_client.list_services(
                cluster=cluster_name
            )
            
            service_arns = services_response.get('serviceArns', [])
            
            # Check each service health
            unhealthy_services = []
            if service_arns:
                services_detail = self.ecs_client.describe_services(
                    cluster=cluster_name,
                    services=service_arns
                )
                
                for service in services_detail['services']:
                    if service['runningCount'] != service['desiredCount']:
                        unhealthy_services.append({
                            'name': service['serviceName'],
                            'running': service['runningCount'],
                            'desired': service['desiredCount']
                        })
            
            # Check container instances
            instance_response = self.ecs_client.list_container_instances(
                cluster=cluster_name
            )
            
            instance_arns = instance_response.get('containerInstanceArns', [])
            unhealthy_instances = []
            
            if instance_arns:
                instances_detail = self.ecs_client.describe_container_instances(
                    cluster=cluster_name,
                    containerInstances=instance_arns
                )
                
                for instance in instances_detail['containerInstances']:
                    if instance['status'] != 'ACTIVE' or not instance['agentConnected']:
                        unhealthy_instances.append({
                            'id': instance['containerInstanceArn'].split('/')[-1],
                            'status': instance['status'],
                            'agent_connected': instance['agentConnected']
                        })
            
            is_healthy = len(unhealthy_services) == 0 and len(unhealthy_instances) == 0
            
            return {
                'healthy': is_healthy,
                'cluster_name': cluster['clusterName'],
                'status': cluster['status'],
                'registered_instances': cluster['registeredContainerInstancesCount'],
                'running_tasks': cluster['runningTasksCount'],
                'pending_tasks': cluster['pendingTasksCount'],
                'active_services': cluster['activeServicesCount'],
                'unhealthy_services': unhealthy_services,
                'unhealthy_instances': unhealthy_instances
            }
            
        except ClientError as e:
            return {'healthy': False, 'error': str(e)}
    
    def check_target_group_health(self, target_group_arn: str) -> Dict:
        """Check ALB target group health"""
        try:
            response = self.elb_client.describe_target_health(
                TargetGroupArn=target_group_arn
            )
            
            total_targets = len(response['TargetHealthDescriptions'])
            healthy_targets = sum(
                1 for t in response['TargetHealthDescriptions']
                if t['TargetHealth']['State'] == 'healthy'
            )
            
            health_percentage = (healthy_targets / total_targets * 100) if total_targets > 0 else 0
            
            return {
                'healthy': health_percentage >= 80,  # Consider healthy if 80% or more targets are healthy
                'target_group_arn': target_group_arn,
                'total_targets': total_targets,
                'healthy_targets': healthy_targets,
                'health_percentage': health_percentage,
                'unhealthy_targets': [
                    {
                        'target': t['Target'],
                        'state': t['TargetHealth']['State'],
                        'reason': t['TargetHealth'].get('Reason', 'N/A'),
                        'description': t['TargetHealth'].get('Description', 'N/A')
                    }
                    for t in response['TargetHealthDescriptions']
                    if t['TargetHealth']['State'] != 'healthy'
                ]
            }
            
        except ClientError as e:
            return {'healthy': False, 'error': str(e)}
    
    def check_cloudwatch_alarms(self, alarm_names: List[str]) -> Dict:
        """Check CloudWatch alarms status"""
        try:
            response = self.cloudwatch_client.describe_alarms(
                AlarmNames=alarm_names
            )
            
            alarms_in_alarm = []
            for alarm in response['MetricAlarms']:
                if alarm['StateValue'] == 'ALARM':
                    alarms_in_alarm.append({
                        'name': alarm['AlarmName'],
                        'state': alarm['StateValue'],
                        'reason': alarm['StateReason']
                    })
            
            return {
                'healthy': len(alarms_in_alarm) == 0,
                'total_alarms': len(response['MetricAlarms']),
                'alarms_in_alarm_state': alarms_in_alarm
            }
            
        except ClientError as e:
            return {'healthy': False, 'error': str(e)}
    
    def comprehensive_health_check(
        self,
        cluster_name: str,
        config: HealthCheckConfig
    ) -> Tuple[bool, Dict]:
        """Perform comprehensive health check"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'cluster': cluster_name,
            'checks': {}
        }
        
        # Check cluster health
        cluster_health = self.check_cluster_health(cluster_name)
        results['checks']['cluster'] = cluster_health
        
        # Check target groups if provided
        if config.alb_target_group_arns:
            results['checks']['target_groups'] = []
            for tg_arn in config.alb_target_group_arns:
                tg_health = self.check_target_group_health(tg_arn)
                results['checks']['target_groups'].append(tg_health)
        
        # Check CloudWatch alarms if provided
        if config.cloudwatch_alarms:
            alarm_health = self.check_cloudwatch_alarms(config.cloudwatch_alarms)
            results['checks']['alarms'] = alarm_health
        
        # Determine overall health
        overall_healthy = all([
            cluster_health.get('healthy', False),
            all(tg.get('healthy', False) for tg in results['checks'].get('target_groups', [{'healthy': True}])),
            results['checks'].get('alarms', {'healthy': True}).get('healthy', True)
        ])
        
        return overall_healthy, results


class RollbackManager:
    """Manages rollback operations for failed re-rolls"""
    
    def __init__(self, region: str = 'us-east-1'):
        self.region = region
        self.asg_client = boto3.client('autoscaling', region_name=region)
        self.ec2_client = boto3.client('ec2', region_name=region)
        self.ecs_client = boto3.client('ecs', region_name=region)
    
    def cancel_instance_refresh(self, asg_name: str) -> bool:
        """Cancel an in-progress instance refresh"""
        try:
            response = self.asg_client.cancel_instance_refresh(
                AutoScalingGroupName=asg_name
            )
            
            print(f"Instance refresh cancelled: {response['InstanceRefreshId']}")
            return True
            
        except ClientError as e:
            if 'No in progress or pending Instance Refresh found' in str(e):
                print("No active instance refresh to cancel")
                return True
            print(f"Failed to cancel instance refresh: {e}")
            return False
    
    def rollback_to_previous_ami(
        self,
        asg_name: str,
        previous_ami_id: str,
        template_id: Optional[str] = None
    ) -> bool:
        """Rollback ASG to use previous AMI"""
        try:
            # Cancel any in-progress refresh first
            self.cancel_instance_refresh(asg_name)
            
            # Wait a bit for cancellation to take effect
            time.sleep(5)
            
            if template_id:
                # Create new launch template version with previous AMI
                response = self.ec2_client.create_launch_template_version(
                    LaunchTemplateId=template_id,
                    SourceVersion='$Latest',
                    LaunchTemplateData={'ImageId': previous_ami_id}
                )
                
                print(f"Created rollback launch template version: {response['LaunchTemplateVersion']['VersionNumber']}")
            
            # Start new instance refresh with previous AMI
            refresh_response = self.asg_client.start_instance_refresh(
                AutoScalingGroupName=asg_name,
                Strategy='Rolling',
                DesiredConfiguration={
                    'LaunchTemplate': {
                        'Version': '$Latest'
                    }
                },
                Preferences={
                    'MinHealthyPercentage': 90,
                    'InstanceWarmup': 180
                }
            )
            
            print(f"Rollback instance refresh started: {refresh_response['InstanceRefreshId']}")
            return True
            
        except ClientError as e:
            print(f"Failed to rollback: {e}")
            return False
    
    def restore_ecs_container_instances(self, cluster_name: str) -> bool:
        """Set all DRAINING container instances back to ACTIVE"""
        try:
            # List all DRAINING instances
            response = self.ecs_client.list_container_instances(
                cluster=cluster_name,
                status='DRAINING'
            )
            
            instance_arns = response.get('containerInstanceArns', [])
            
            if not instance_arns:
                print("No DRAINING instances to restore")
                return True
            
            # Update instances back to ACTIVE
            batch_size = 10
            for i in range(0, len(instance_arns), batch_size):
                batch = instance_arns[i:i + batch_size]
                self.ecs_client.update_container_instances_state(
                    cluster=cluster_name,
                    containerInstances=batch,
                    status='ACTIVE'
                )
            
            print(f"Restored {len(instance_arns)} container instances to ACTIVE")
            return True
            
        except ClientError as e:
            print(f"Failed to restore container instances: {e}")
            return False


class RerollReporter:
    """Generate reports for re-roll operations"""
    
    def __init__(self):
        self.report_data = {}
    
    def generate_summary_report(
        self,
        operation_id: str,
        status: Dict,
        health_check_results: Optional[Dict] = None
    ) -> str:
        """Generate a summary report of the re-roll operation"""
        
        report = []
        report.append("=" * 80)
        report.append(f"ECS RE-ROLL OPERATION REPORT")
        report.append(f"Operation ID: {operation_id}")
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append("=" * 80)
        report.append("")
        
        # Operation Status
        report.append("OPERATION STATUS")
        report.append("-" * 40)
        report.append(f"Status: {status.get('status', 'UNKNOWN')}")
        report.append(f"Start Time: {status.get('start_time', 'N/A')}")
        
        if 'refresh_status' in status:
            refresh = status['refresh_status']
            report.append(f"Progress: {refresh.get('percentage_complete', 0)}%")
            if refresh.get('end_time'):
                report.append(f"End Time: {refresh['end_time']}")
        
        report.append("")
        
        # Configuration
        report.append("CONFIGURATION")
        report.append("-" * 40)
        report.append(f"Cluster: {status.get('cluster', 'N/A')}")
        report.append(f"ASG: {status.get('asg', 'N/A')}")
        report.append(f"New AMI: {status.get('new_ami', 'N/A')}")
        report.append("")
        
        # Steps
        if 'steps' in status:
            report.append("EXECUTION STEPS")
            report.append("-" * 40)
            for step in status['steps']:
                report.append(f"• {step['name']}: {step.get('status', 'unknown')} at {step.get('timestamp', 'N/A')}")
            report.append("")
        
        # ECS Status
        if 'ecs_status' in status:
            ecs = status['ecs_status']
            report.append("ECS STATUS")
            report.append("-" * 40)
            report.append(f"Running Tasks: {ecs.get('running_tasks', 'N/A')}")
            report.append(f"Pending Tasks: {ecs.get('pending_tasks', 'N/A')}")
            report.append(f"Draining Instances: {ecs.get('draining_instances', 'N/A')}")
            report.append("")
        
        # Health Check Results
        if health_check_results:
            report.append("HEALTH CHECK RESULTS")
            report.append("-" * 40)
            
            if 'cluster' in health_check_results.get('checks', {}):
                cluster_health = health_check_results['checks']['cluster']
                report.append(f"Cluster Health: {'✓ HEALTHY' if cluster_health.get('healthy') else '✗ UNHEALTHY'}")
                
                if cluster_health.get('unhealthy_services'):
                    report.append("  Unhealthy Services:")
                    for svc in cluster_health['unhealthy_services']:
                        report.append(f"    - {svc['name']}: {svc['running']}/{svc['desired']} running")
            
            if 'target_groups' in health_check_results.get('checks', {}):
                report.append("Target Group Health:")
                for tg in health_check_results['checks']['target_groups']:
                    health_pct = tg.get('health_percentage', 0)
                    report.append(f"  - {tg.get('target_group_arn', 'N/A').split('/')[-1]}: {health_pct:.1f}% healthy")
            
            report.append("")
        
        # Recommendations
        report.append("RECOMMENDATIONS")
        report.append("-" * 40)
        
        if status.get('status') == 'COMPLETED':
            report.append("✓ Re-roll completed successfully")
            report.append("• Monitor services for the next 30 minutes")
            report.append("• Review CloudWatch metrics and logs")
            report.append("• Verify application functionality")
        elif status.get('status') == 'FAILED':
            report.append("✗ Re-roll failed")
            report.append("• Review error logs for root cause")
            report.append("• Consider rollback if services are impacted")
            report.append("• Contact support if issue persists")
        else:
            report.append("⚠ Re-roll in progress")
            report.append("• Continue monitoring")
            report.append("• Be prepared for rollback if issues arise")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_report(self, report: str, filename: str):
        """Save report to file"""
        with open(filename, 'w') as f:
            f.write(report)
        print(f"Report saved to: {filename}")


# Example usage
def example_health_check():
    """Example of performing health checks"""
    
    health_checker = ECSHealthChecker(region='us-east-1')
    
    # Configure health checks
    config = HealthCheckConfig(
        alb_target_group_arns=[
            'arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-targets/abc123'
        ],
        cloudwatch_alarms=[
            'HighCPUAlarm',
            'LowMemoryAlarm'
        ]
    )
    
    # Perform comprehensive health check
    is_healthy, results = health_checker.comprehensive_health_check(
        cluster_name='my-cluster',
        config=config
    )
    
    print(f"Overall Health: {'✓ HEALTHY' if is_healthy else '✗ UNHEALTHY'}")
    print(json.dumps(results, indent=2))
    
    return is_healthy, results


def example_rollback():
    """Example of performing rollback"""
    
    rollback_manager = RollbackManager(region='us-east-1')
    
    # Cancel current refresh and rollback
    success = rollback_manager.rollback_to_previous_ami(
        asg_name='my-asg',
        previous_ami_id='ami-previous123',
        template_id='lt-123456'
    )
    
    if success:
        # Restore ECS instances
        rollback_manager.restore_ecs_container_instances('my-cluster')
    
    return success


if __name__ == "__main__":
    # Run example health check
    example_health_check()

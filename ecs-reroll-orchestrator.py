"""
ECS Cluster Re-roll Orchestrator
Handles ECS cluster re-rolling with EC2 instance replacement while managing AWS token expiration
"""

import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from botocore.exceptions import ClientError, TokenRefreshError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RerollStatus(Enum):
    """Status enumeration for re-roll operations"""
    INITIATED = "INITIATED"
    DRAINING_TASKS = "DRAINING_TASKS"
    REFRESHING_INSTANCES = "REFRESHING_INSTANCES"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class RerollConfig:
    """Configuration for re-roll operations"""
    cluster_name: str
    asg_name: str
    new_ami_id: str
    role_arn: str
    external_id: Optional[str] = None
    session_name: str = "ecs-reroll-session"
    drain_timeout_minutes: int = 30
    instance_warmup_seconds: int = 300
    min_healthy_percentage: int = 90
    max_healthy_percentage: int = 200
    region: str = "us-east-1"


class AWSSessionManager:
    """Manages AWS sessions with automatic token refresh"""
    
    def __init__(self, config: RerollConfig):
        self.config = config
        self._session_cache = {}
        self._session_expiry = {}
        
    def get_session(self, force_refresh: bool = False) -> Dict[str, boto3.client]:
        """Get or create AWS session with required clients"""
        current_time = datetime.now()
        session_key = self.config.role_arn
        
        # Check if we need to refresh the session
        if (force_refresh or 
            session_key not in self._session_cache or 
            session_key not in self._session_expiry or
            current_time >= self._session_expiry[session_key]):
            
            logger.info(f"Refreshing AWS session for role: {self.config.role_arn}")
            self._create_session(session_key)
            
        return self._session_cache[session_key]
    
    def _create_session(self, session_key: str):
        """Create new assumed role session"""
        try:
            # Create STS client with current credentials
            sts = boto3.client('sts', region_name=self.config.region)
            
            # Assume role
            assume_role_params = {
                'RoleArn': self.config.role_arn,
                'RoleSessionName': self.config.session_name,
                'DurationSeconds': 3600  # 1 hour
            }
            
            if self.config.external_id:
                assume_role_params['ExternalId'] = self.config.external_id
                
            response = sts.assume_role(**assume_role_params)
            credentials = response['Credentials']
            
            # Create new session with assumed role credentials
            session = boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=self.config.region
            )
            
            # Create required clients
            clients = {
                'ecs': session.client('ecs'),
                'ec2': session.client('ec2'),
                'autoscaling': session.client('autoscaling')
            }
            
            # Cache session and set expiry (5 minutes before actual expiry for safety)
            self._session_cache[session_key] = clients
            self._session_expiry[session_key] = datetime.now() + timedelta(minutes=25)
            
            logger.info(f"Successfully created session, expires at: {self._session_expiry[session_key]}")
            
        except ClientError as e:
            logger.error(f"Failed to assume role: {e}")
            raise


class ECSRerollOrchestrator:
    """Main orchestrator for ECS cluster re-roll operations"""
    
    def __init__(self, config: RerollConfig):
        self.config = config
        self.session_manager = AWSSessionManager(config)
        self.operation_id = None
        self.operation_status = {}
        
    def initiate_reroll(self) -> str:
        """
        Quickly initiate the re-roll process and return operation ID
        This method completes quickly to avoid token timeout
        """
        try:
            self.operation_id = f"reroll-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            logger.info(f"Initiating re-roll operation: {self.operation_id}")
            
            # Get AWS clients
            clients = self.session_manager.get_session()
            
            # Initialize operation status
            self.operation_status = {
                'operation_id': self.operation_id,
                'status': RerollStatus.INITIATED.value,
                'start_time': datetime.now().isoformat(),
                'cluster': self.config.cluster_name,
                'asg': self.config.asg_name,
                'new_ami': self.config.new_ami_id,
                'steps': []
            }
            
            # Step 1: Set ECS container instances to DRAINING
            self._initiate_ecs_draining(clients['ecs'])
            
            # Step 2: Start ASG instance refresh (non-blocking)
            self._initiate_instance_refresh(clients['autoscaling'])
            
            # Save operation status to file for polling
            self._save_operation_status()
            
            logger.info(f"Re-roll operation {self.operation_id} initiated successfully")
            return self.operation_id
            
        except Exception as e:
            logger.error(f"Failed to initiate re-roll: {e}")
            self.operation_status['status'] = RerollStatus.FAILED.value
            self.operation_status['error'] = str(e)
            self._save_operation_status()
            raise
    
    def _initiate_ecs_draining(self, ecs_client):
        """Set all container instances in the cluster to DRAINING"""
        try:
            logger.info(f"Setting container instances to DRAINING for cluster: {self.config.cluster_name}")
            
            # List all container instances
            paginator = ecs_client.get_paginator('list_container_instances')
            instance_arns = []
            
            for page in paginator.paginate(cluster=self.config.cluster_name):
                instance_arns.extend(page.get('containerInstanceArns', []))
            
            if not instance_arns:
                logger.warning("No container instances found in cluster")
                return
            
            # Update container instances to DRAINING in batches
            batch_size = 10
            for i in range(0, len(instance_arns), batch_size):
                batch = instance_arns[i:i + batch_size]
                ecs_client.update_container_instances_state(
                    cluster=self.config.cluster_name,
                    containerInstances=batch,
                    status='DRAINING'
                )
            
            self.operation_status['steps'].append({
                'name': 'drain_ecs_instances',
                'status': 'initiated',
                'timestamp': datetime.now().isoformat(),
                'instance_count': len(instance_arns)
            })
            
            logger.info(f"Successfully initiated draining for {len(instance_arns)} instances")
            
        except ClientError as e:
            logger.error(f"Failed to initiate ECS draining: {e}")
            raise
    
    def _initiate_instance_refresh(self, asg_client):
        """Start ASG instance refresh with new AMI"""
        try:
            logger.info(f"Starting instance refresh for ASG: {self.config.asg_name}")
            
            # Update launch template or launch configuration with new AMI
            self._update_asg_ami(asg_client)
            
            # Start instance refresh
            response = asg_client.start_instance_refresh(
                AutoScalingGroupName=self.config.asg_name,
                Strategy='Rolling',
                DesiredConfiguration={
                    'LaunchTemplate': {
                        'Version': '$Latest'
                    }
                },
                Preferences={
                    'MinHealthyPercentage': self.config.min_healthy_percentage,
                    'InstanceWarmup': self.config.instance_warmup_seconds,
                    'CheckpointPercentages': [50, 100],
                    'CheckpointDelay': 600  # 10 minutes between checkpoints
                }
            )
            
            self.operation_status['steps'].append({
                'name': 'instance_refresh',
                'status': 'initiated',
                'timestamp': datetime.now().isoformat(),
                'refresh_id': response['InstanceRefreshId']
            })
            
            logger.info(f"Instance refresh started with ID: {response['InstanceRefreshId']}")
            
        except ClientError as e:
            logger.error(f"Failed to start instance refresh: {e}")
            raise
    
    def _update_asg_ami(self, asg_client):
        """Update ASG launch template with new AMI"""
        try:
            # Get ASG details
            response = asg_client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[self.config.asg_name]
            )
            
            if not response['AutoScalingGroups']:
                raise ValueError(f"ASG {self.config.asg_name} not found")
            
            asg = response['AutoScalingGroups'][0]
            
            # Check if using launch template
            if 'LaunchTemplate' in asg:
                lt_id = asg['LaunchTemplate']['LaunchTemplateId']
                self._update_launch_template(lt_id)
            elif 'LaunchConfigurationName' in asg:
                # Handle launch configuration (legacy)
                self._create_new_launch_configuration(asg_client, asg['LaunchConfigurationName'])
            else:
                raise ValueError("ASG has neither launch template nor launch configuration")
                
        except Exception as e:
            logger.error(f"Failed to update ASG AMI: {e}")
            raise
    
    def _update_launch_template(self, template_id: str):
        """Create new launch template version with updated AMI"""
        try:
            clients = self.session_manager.get_session()
            ec2_client = clients['ec2']
            
            # Get current launch template
            response = ec2_client.describe_launch_template_versions(
                LaunchTemplateId=template_id,
                Versions=['$Latest']
            )
            
            current_version = response['LaunchTemplateVersions'][0]['LaunchTemplateData']
            
            # Update with new AMI
            current_version['ImageId'] = self.config.new_ami_id
            
            # Create new version
            new_version_response = ec2_client.create_launch_template_version(
                LaunchTemplateId=template_id,
                SourceVersion='$Latest',
                LaunchTemplateData={'ImageId': self.config.new_ami_id}
            )
            
            logger.info(f"Created new launch template version: {new_version_response['LaunchTemplateVersion']['VersionNumber']}")
            
        except ClientError as e:
            logger.error(f"Failed to update launch template: {e}")
            raise
    
    def _save_operation_status(self):
        """Save operation status to file for polling"""
        status_file = f"/tmp/{self.operation_id}-status.json"
        with open(status_file, 'w') as f:
            json.dump(self.operation_status, f, indent=2)
        logger.info(f"Operation status saved to: {status_file}")


class ECSRerollMonitor:
    """Monitor and poll re-roll operation status"""
    
    def __init__(self, config: RerollConfig, operation_id: str):
        self.config = config
        self.operation_id = operation_id
        self.session_manager = AWSSessionManager(config)
        self.status_file = f"/tmp/{operation_id}-status.json"
        
    def poll_status(self) -> Dict:
        """Poll current status of re-roll operation"""
        try:
            # Load operation status
            with open(self.status_file, 'r') as f:
                status = json.load(f)
            
            # Get fresh AWS clients with token refresh if needed
            clients = self.session_manager.get_session()
            
            # Check ECS draining status
            ecs_status = self._check_ecs_status(clients['ecs'])
            
            # Check instance refresh status
            refresh_status = self._check_instance_refresh_status(clients['autoscaling'])
            
            # Update overall status
            status['ecs_status'] = ecs_status
            status['refresh_status'] = refresh_status
            status['last_check'] = datetime.now().isoformat()
            
            # Determine overall status
            if refresh_status.get('status') == 'Successful' and ecs_status.get('tasks_running') == 0:
                status['status'] = RerollStatus.COMPLETED.value
            elif refresh_status.get('status') == 'Failed':
                status['status'] = RerollStatus.FAILED.value
            elif refresh_status.get('status') in ['Pending', 'InProgress']:
                status['status'] = RerollStatus.REFRESHING_INSTANCES.value
            
            # Save updated status
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=2)
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to poll status: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def _check_ecs_status(self, ecs_client) -> Dict:
        """Check ECS cluster and task status"""
        try:
            # Get cluster status
            cluster_response = ecs_client.describe_clusters(
                clusters=[self.config.cluster_name]
            )
            
            if not cluster_response['clusters']:
                return {'error': 'Cluster not found'}
            
            cluster = cluster_response['clusters'][0]
            
            # Get running tasks count
            tasks_response = ecs_client.list_tasks(
                cluster=self.config.cluster_name,
                desiredStatus='RUNNING'
            )
            
            return {
                'running_tasks': cluster['runningTasksCount'],
                'pending_tasks': cluster['pendingTasksCount'],
                'active_services': cluster['activeServicesCount'],
                'draining_instances': self._count_draining_instances(ecs_client),
                'tasks_running': len(tasks_response.get('taskArns', []))
            }
            
        except ClientError as e:
            logger.error(f"Failed to check ECS status: {e}")
            return {'error': str(e)}
    
    def _count_draining_instances(self, ecs_client) -> int:
        """Count instances in DRAINING state"""
        try:
            paginator = ecs_client.get_paginator('list_container_instances')
            draining_count = 0
            
            for page in paginator.paginate(
                cluster=self.config.cluster_name,
                status='DRAINING'
            ):
                draining_count += len(page.get('containerInstanceArns', []))
            
            return draining_count
            
        except ClientError:
            return 0
    
    def _check_instance_refresh_status(self, asg_client) -> Dict:
        """Check ASG instance refresh status"""
        try:
            response = asg_client.describe_instance_refreshes(
                AutoScalingGroupName=self.config.asg_name,
                MaxRecords=1
            )
            
            if not response['InstanceRefreshes']:
                return {'status': 'No refresh found'}
            
            refresh = response['InstanceRefreshes'][0]
            
            return {
                'status': refresh['Status'],
                'percentage_complete': refresh.get('PercentageComplete', 0),
                'instances_to_update': refresh.get('InstancesToUpdate', 0),
                'start_time': refresh.get('StartTime', '').isoformat() if 'StartTime' in refresh else None,
                'end_time': refresh.get('EndTime', '').isoformat() if 'EndTime' in refresh else None
            }
            
        except ClientError as e:
            logger.error(f"Failed to check instance refresh status: {e}")
            return {'error': str(e)}
    
    def wait_for_completion(self, poll_interval_seconds: int = 60, max_wait_minutes: int = 120) -> bool:
        """
        Wait for re-roll operation to complete
        Returns True if successful, False if failed or timed out
        """
        start_time = datetime.now()
        max_wait = timedelta(minutes=max_wait_minutes)
        
        while datetime.now() - start_time < max_wait:
            status = self.poll_status()
            
            logger.info(f"Current status: {status.get('status')}")
            
            if status.get('status') == RerollStatus.COMPLETED.value:
                logger.info("Re-roll operation completed successfully")
                return True
            elif status.get('status') == RerollStatus.FAILED.value:
                logger.error("Re-roll operation failed")
                return False
            
            # Log progress
            if 'refresh_status' in status:
                logger.info(f"Instance refresh progress: {status['refresh_status'].get('percentage_complete', 0)}%")
            if 'ecs_status' in status:
                logger.info(f"Running tasks: {status['ecs_status'].get('running_tasks', 'unknown')}")
            
            time.sleep(poll_interval_seconds)
        
        logger.error(f"Re-roll operation timed out after {max_wait_minutes} minutes")
        return False


# Example usage
def main():
    """Example usage of the ECS re-roll orchestrator"""
    
    # Configuration - using existing AWS credentials
    # Credentials can be provided via:
    # - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
    # - AWS credentials file: ~/.aws/credentials
    # - IAM instance role (if running on EC2)
    config = RerollConfig(
        cluster_name="my-ecs-cluster",
        asg_name="my-asg",
        new_ami_id="ami-0123456789abcdef0",
        region="us-east-1"
    )
    
    # Step 1: Quickly initiate re-roll (completes in seconds)
    orchestrator = ECSRerollOrchestrator(config)
    operation_id = orchestrator.initiate_reroll()
    print(f"Re-roll initiated with operation ID: {operation_id}")
    
    # Step 2: Monitor progress (can be run separately or in different process)
    monitor = ECSRerollMonitor(config, operation_id)
    
    # Option A: Poll once for status
    status = monitor.poll_status()
    print(f"Current status: {json.dumps(status, indent=2)}")
    
    # Option B: Wait for completion with automatic polling
    # success = monitor.wait_for_completion(poll_interval_seconds=60, max_wait_minutes=90)
    # print(f"Re-roll {'succeeded' if success else 'failed'}")


if __name__ == "__main__":
    main()

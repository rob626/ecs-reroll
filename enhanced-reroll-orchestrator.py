"""
Enhanced ECS Cluster Re-roll Orchestrator with Container Restart Management
Ensures proper task redistribution and service stabilization after infrastructure updates
"""

import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ServiceRecoveryConfig:
    """Configuration for service recovery after re-roll"""
    wait_for_service_stability: bool = True
    service_stability_timeout_minutes: int = 15
    force_new_deployment: bool = False
    update_service_desired_count: bool = True
    pre_drain_service_snapshot: Dict = field(default_factory=dict)
    scale_up_percentage: int = 0  # Temporary scale up during migration (0 = no scale up)


class EnhancedECSRerollOrchestrator:
    """Enhanced orchestrator with container restart management"""
    
    def __init__(self, config, recovery_config: Optional[ServiceRecoveryConfig] = None):
        self.config = config
        self.recovery_config = recovery_config or ServiceRecoveryConfig()
        self.session_manager = None  # Would be initialized with AWSSessionManager
        self.operation_id = None
        self.operation_status = {}
        self.service_snapshots = {}
        
    def capture_service_state(self, clients: Dict) -> Dict:
        """Capture current state of all ECS services before re-roll"""
        try:
            ecs_client = clients['ecs']
            logger.info(f"Capturing service state for cluster: {self.config.cluster_name}")
            
            # List all services
            service_arns = []
            paginator = ecs_client.get_paginator('list_services')
            for page in paginator.paginate(cluster=self.config.cluster_name):
                service_arns.extend(page.get('serviceArns', []))
            
            if not service_arns:
                logger.warning("No services found in cluster")
                return {}
            
            # Capture detailed state for each service
            services_response = ecs_client.describe_services(
                cluster=self.config.cluster_name,
                services=service_arns
            )
            
            snapshots = {}
            for service in services_response['services']:
                service_name = service['serviceName']
                snapshots[service_name] = {
                    'service_arn': service['serviceArn'],
                    'desired_count': service['desiredCount'],
                    'running_count': service['runningCount'],
                    'pending_count': service['pendingCount'],
                    'deployment_configuration': service.get('deploymentConfiguration', {}),
                    'task_definition': service['taskDefinition'],
                    'launch_type': service.get('launchType', 'EC2'),
                    'platform_version': service.get('platformVersion'),
                    'network_configuration': service.get('networkConfiguration'),
                    'placement_constraints': service.get('placementConstraints', []),
                    'placement_strategy': service.get('placementStrategy', []),
                    'health_check_grace_period': service.get('healthCheckGracePeriodSeconds'),
                    'enable_execute_command': service.get('enableExecuteCommand', False),
                    'tags': service.get('tags', [])
                }
                
                # Capture running tasks for this service
                tasks_response = ecs_client.list_tasks(
                    cluster=self.config.cluster_name,
                    serviceName=service_name,
                    desiredStatus='RUNNING'
                )
                
                snapshots[service_name]['running_task_arns'] = tasks_response.get('taskArns', [])
                snapshots[service_name]['running_task_count'] = len(snapshots[service_name]['running_task_arns'])
            
            logger.info(f"Captured state for {len(snapshots)} services")
            return snapshots
            
        except ClientError as e:
            logger.error(f"Failed to capture service state: {e}")
            return {}
    
    def initiate_controlled_drain(self, clients: Dict) -> bool:
        """
        Perform controlled draining with optional service scaling
        """
        try:
            ecs_client = clients['ecs']
            
            # First, capture service state
            self.service_snapshots = self.capture_service_state(clients)
            self.recovery_config.pre_drain_service_snapshot = self.service_snapshots
            
            # Optionally scale up services before draining (blue-green style)
            if self.recovery_config.scale_up_percentage > 0:
                self._scale_services_for_migration(ecs_client)
            
            # Get all container instances
            instance_arns = []
            paginator = ecs_client.get_paginator('list_container_instances')
            for page in paginator.paginate(cluster=self.config.cluster_name):
                instance_arns.extend(page.get('containerInstanceArns', []))
            
            if not instance_arns:
                logger.warning("No container instances to drain")
                return True
            
            # Set instances to DRAINING in batches
            logger.info(f"Setting {len(instance_arns)} instances to DRAINING")
            batch_size = 10
            for i in range(0, len(instance_arns), batch_size):
                batch = instance_arns[i:i + batch_size]
                ecs_client.update_container_instances_state(
                    cluster=self.config.cluster_name,
                    containerInstances=batch,
                    status='DRAINING'
                )
            
            # Wait for tasks to start migrating
            time.sleep(10)
            
            # Monitor task migration
            self._monitor_task_migration(ecs_client, timeout_minutes=self.config.drain_timeout_minutes)
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed during controlled drain: {e}")
            return False
    
    def _scale_services_for_migration(self, ecs_client):
        """Temporarily scale up services to maintain capacity during migration"""
        try:
            for service_name, snapshot in self.service_snapshots.items():
                current_desired = snapshot['desired_count']
                if current_desired > 0:
                    # Calculate temporary increased capacity
                    scale_factor = 1 + (self.recovery_config.scale_up_percentage / 100)
                    temp_desired = int(current_desired * scale_factor)
                    
                    if temp_desired > current_desired:
                        logger.info(f"Temporarily scaling {service_name} from {current_desired} to {temp_desired}")
                        
                        ecs_client.update_service(
                            cluster=self.config.cluster_name,
                            service=service_name,
                            desiredCount=temp_desired
                        )
        
        except ClientError as e:
            logger.warning(f"Failed to scale service for migration: {e}")
    
    def _monitor_task_migration(self, ecs_client, timeout_minutes: int):
        """Monitor task migration from draining instances"""
        start_time = datetime.now()
        timeout = timedelta(minutes=timeout_minutes)
        
        while datetime.now() - start_time < timeout:
            # Count tasks on draining instances
            draining_instances = ecs_client.list_container_instances(
                cluster=self.config.cluster_name,
                status='DRAINING'
            )
            
            if not draining_instances.get('containerInstanceArns'):
                logger.info("No more draining instances")
                break
            
            # Check tasks on draining instances
            tasks_on_draining = 0
            for instance_arn in draining_instances['containerInstanceArns']:
                tasks = ecs_client.list_tasks(
                    cluster=self.config.cluster_name,
                    containerInstance=instance_arn
                )
                tasks_on_draining += len(tasks.get('taskArns', []))
            
            logger.info(f"Tasks still running on draining instances: {tasks_on_draining}")
            
            if tasks_on_draining == 0:
                logger.info("All tasks migrated from draining instances")
                break
            
            time.sleep(30)
    
    def verify_new_instances_joined(self, clients: Dict) -> bool:
        """Verify new instances have joined the ECS cluster"""
        try:
            ecs_client = clients['ecs']
            asg_client = clients['autoscaling']
            
            # Get expected instance count from ASG
            asg_response = asg_client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[self.config.asg_name]
            )
            
            if not asg_response['AutoScalingGroups']:
                logger.error("ASG not found")
                return False
            
            asg = asg_response['AutoScalingGroups'][0]
            expected_instances = asg['DesiredCapacity']
            
            # Wait for instances to register with ECS
            max_wait = 10  # minutes
            start_time = datetime.now()
            
            while (datetime.now() - start_time).total_seconds() < max_wait * 60:
                # Count active ECS container instances
                active_instances = ecs_client.list_container_instances(
                    cluster=self.config.cluster_name,
                    status='ACTIVE'
                )
                
                active_count = len(active_instances.get('containerInstanceArns', []))
                
                logger.info(f"Active ECS instances: {active_count}/{expected_instances}")
                
                if active_count >= expected_instances:
                    logger.info("All expected instances have joined the cluster")
                    return True
                
                time.sleep(30)
            
            logger.warning(f"Timeout waiting for instances to join. Got {active_count}/{expected_instances}")
            return False
            
        except ClientError as e:
            logger.error(f"Failed to verify new instances: {e}")
            return False
    
    def restart_and_rebalance_services(self, clients: Dict) -> bool:
        """
        Restart services and ensure tasks are properly distributed on new instances
        """
        try:
            ecs_client = clients['ecs']
            logger.info("Starting service restart and rebalancing")
            
            # First, ensure new instances are ready
            if not self.verify_new_instances_joined(clients):
                logger.warning("Not all instances ready, but proceeding with service restart")
            
            # Get current services
            service_arns = []
            paginator = ecs_client.get_paginator('list_services')
            for page in paginator.paginate(cluster=self.config.cluster_name):
                service_arns.extend(page.get('serviceArns', []))
            
            if not service_arns:
                logger.warning("No services to restart")
                return True
            
            # Process each service
            for service_arn in service_arns:
                service_name = service_arn.split('/')[-1]
                
                try:
                    # Get service details
                    service_response = ecs_client.describe_services(
                        cluster=self.config.cluster_name,
                        services=[service_arn]
                    )
                    
                    if not service_response['services']:
                        continue
                    
                    service = service_response['services'][0]
                    
                    # Restore original desired count if we have a snapshot
                    if service_name in self.service_snapshots:
                        original_desired = self.service_snapshots[service_name]['desired_count']
                        current_desired = service['desiredCount']
                        
                        if current_desired != original_desired:
                            logger.info(f"Restoring {service_name} desired count to {original_desired}")
                            ecs_client.update_service(
                                cluster=self.config.cluster_name,
                                service=service_name,
                                desiredCount=original_desired
                            )
                    
                    # Force new deployment if configured
                    if self.recovery_config.force_new_deployment:
                        logger.info(f"Forcing new deployment for service: {service_name}")
                        ecs_client.update_service(
                            cluster=self.config.cluster_name,
                            service=service_name,
                            forceNewDeployment=True
                        )
                    
                    # Stop tasks on any remaining draining instances
                    self._stop_tasks_on_draining_instances(ecs_client, service_name)
                    
                except ClientError as e:
                    logger.error(f"Failed to restart service {service_name}: {e}")
                    continue
            
            # Wait for service stability
            if self.recovery_config.wait_for_service_stability:
                return self._wait_for_services_stable(ecs_client)
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to restart services: {e}")
            return False
    
    def _stop_tasks_on_draining_instances(self, ecs_client, service_name: str):
        """Stop any tasks still running on draining instances"""
        try:
            # Get draining instances
            draining_response = ecs_client.list_container_instances(
                cluster=self.config.cluster_name,
                status='DRAINING'
            )
            
            draining_instances = draining_response.get('containerInstanceArns', [])
            
            for instance_arn in draining_instances:
                # List tasks on this instance for this service
                tasks_response = ecs_client.list_tasks(
                    cluster=self.config.cluster_name,
                    containerInstance=instance_arn,
                    serviceName=service_name
                )
                
                task_arns = tasks_response.get('taskArns', [])
                
                if task_arns:
                    logger.info(f"Stopping {len(task_arns)} tasks for {service_name} on draining instance")
                    
                    for task_arn in task_arns:
                        try:
                            ecs_client.stop_task(
                                cluster=self.config.cluster_name,
                                task=task_arn,
                                reason='Re-roll: Moving task to new instance'
                            )
                        except ClientError:
                            pass  # Task might already be stopping
                            
        except ClientError as e:
            logger.warning(f"Failed to stop tasks on draining instances: {e}")
    
    def _wait_for_services_stable(self, ecs_client) -> bool:
        """Wait for all services to reach stable state"""
        logger.info("Waiting for services to stabilize...")
        
        start_time = datetime.now()
        timeout = timedelta(minutes=self.recovery_config.service_stability_timeout_minutes)
        last_status = {}
        
        while datetime.now() - start_time < timeout:
            try:
                # List all services
                service_arns = []
                paginator = ecs_client.get_paginator('list_services')
                for page in paginator.paginate(cluster=self.config.cluster_name):
                    service_arns.extend(page.get('serviceArns', []))
                
                if not service_arns:
                    logger.info("No services to check")
                    return True
                
                # Check each service
                services_response = ecs_client.describe_services(
                    cluster=self.config.cluster_name,
                    services=service_arns
                )
                
                all_stable = True
                unstable_services = []
                
                for service in services_response['services']:
                    service_name = service['serviceName']
                    desired = service['desiredCount']
                    running = service['runningCount']
                    pending = service['pendingCount']
                    
                    is_stable = (running == desired and pending == 0)
                    
                    if not is_stable:
                        all_stable = False
                        unstable_services.append(f"{service_name} ({running}/{desired} running, {pending} pending)")
                    
                    # Log status changes
                    current_status = f"{running}/{desired}/{pending}"
                    if last_status.get(service_name) != current_status:
                        logger.info(f"Service {service_name}: {running}/{desired} running, {pending} pending")
                        last_status[service_name] = current_status
                
                if all_stable:
                    logger.info("✓ All services have stabilized")
                    return True
                
                if unstable_services and (datetime.now() - start_time).total_seconds() % 60 < 30:
                    logger.info(f"Waiting for {len(unstable_services)} services to stabilize: {', '.join(unstable_services[:3])}")
                
                time.sleep(30)
                
            except ClientError as e:
                logger.error(f"Error checking service stability: {e}")
                time.sleep(30)
        
        logger.warning(f"Services did not stabilize within {self.recovery_config.service_stability_timeout_minutes} minutes")
        return False
    
    def cleanup_draining_instances(self, clients: Dict) -> bool:
        """Clean up any remaining draining instances"""
        try:
            ecs_client = clients['ecs']
            
            # Get all draining instances
            draining_response = ecs_client.list_container_instances(
                cluster=self.config.cluster_name,
                status='DRAINING'
            )
            
            draining_instances = draining_response.get('containerInstanceArns', [])
            
            if not draining_instances:
                logger.info("No draining instances to clean up")
                return True
            
            logger.info(f"Found {len(draining_instances)} draining instances to clean up")
            
            # Deregister draining instances from ECS
            for instance_arn in draining_instances:
                try:
                    # First check if any tasks are still running
                    tasks_response = ecs_client.list_tasks(
                        cluster=self.config.cluster_name,
                        containerInstance=instance_arn
                    )
                    
                    if tasks_response.get('taskArns'):
                        logger.warning(f"Instance {instance_arn} still has {len(tasks_response['taskArns'])} tasks")
                        continue
                    
                    # Deregister the instance
                    ecs_client.deregister_container_instance(
                        cluster=self.config.cluster_name,
                        containerInstance=instance_arn,
                        force=True
                    )
                    logger.info(f"Deregistered instance: {instance_arn}")
                    
                except ClientError as e:
                    logger.warning(f"Failed to deregister instance {instance_arn}: {e}")
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed during cleanup: {e}")
            return False
    
    def perform_complete_reroll(self) -> str:
        """
        Perform complete re-roll with proper container restart management
        """
        try:
            self.operation_id = f"reroll-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            logger.info(f"Starting complete re-roll operation: {self.operation_id}")
            
            # Get AWS clients
            clients = self.session_manager.get_session()
            
            # Initialize operation status
            self.operation_status = {
                'operation_id': self.operation_id,
                'status': 'INITIATED',
                'start_time': datetime.now().isoformat(),
                'cluster': self.config.cluster_name,
                'asg': self.config.asg_name,
                'new_ami': self.config.new_ami_id,
                'steps': []
            }
            
            # Step 1: Capture pre-reroll state
            logger.info("Step 1: Capturing service state")
            self.service_snapshots = self.capture_service_state(clients)
            self.operation_status['service_snapshots'] = len(self.service_snapshots)
            
            # Step 2: Controlled drain with optional scaling
            logger.info("Step 2: Initiating controlled drain")
            if not self.initiate_controlled_drain(clients):
                raise Exception("Failed to initiate controlled drain")
            
            # Step 3: Start instance refresh
            logger.info("Step 3: Starting instance refresh")
            self._initiate_instance_refresh(clients['autoscaling'])
            
            # Step 4: Monitor instance refresh completion
            logger.info("Step 4: Monitoring instance refresh")
            # This would typically be done by the monitor component
            
            # Step 5: Verify new instances joined
            logger.info("Step 5: Verifying new instances")
            if not self.verify_new_instances_joined(clients):
                logger.warning("Not all instances joined, but continuing")
            
            # Step 6: Restart and rebalance services
            logger.info("Step 6: Restarting and rebalancing services")
            if not self.restart_and_rebalance_services(clients):
                logger.warning("Some services may not be fully stable")
            
            # Step 7: Cleanup
            logger.info("Step 7: Cleaning up draining instances")
            self.cleanup_draining_instances(clients)
            
            self.operation_status['status'] = 'COMPLETED'
            self.operation_status['end_time'] = datetime.now().isoformat()
            
            # Save operation status
            self._save_operation_status()
            
            logger.info(f"Re-roll operation {self.operation_id} completed successfully")
            return self.operation_id
            
        except Exception as e:
            logger.error(f"Re-roll operation failed: {e}")
            self.operation_status['status'] = 'FAILED'
            self.operation_status['error'] = str(e)
            self._save_operation_status()
            raise
    
    def _initiate_instance_refresh(self, asg_client):
        """Start ASG instance refresh (placeholder - would use actual implementation)"""
        # This would use the actual implementation from the original orchestrator
        pass
    
    def _save_operation_status(self):
        """Save operation status to file"""
        status_file = f"/tmp/{self.operation_id}-status.json"
        with open(status_file, 'w') as f:
            json.dump(self.operation_status, f, indent=2)
        logger.info(f"Operation status saved to: {status_file}")


class ServiceVerification:
    """Verify services are running correctly after re-roll"""
    
    def __init__(self, cluster_name: str, region: str = 'us-east-1'):
        self.cluster_name = cluster_name
        self.ecs_client = boto3.client('ecs', region_name=region)
    
    def verify_task_distribution(self) -> Dict:
        """Verify tasks are evenly distributed across instances"""
        try:
            # Get all active instances
            instances_response = self.ecs_client.list_container_instances(
                cluster=self.cluster_name,
                status='ACTIVE'
            )
            
            instance_arns = instances_response.get('containerInstanceArns', [])
            
            if not instance_arns:
                return {'error': 'No active instances found'}
            
            # Get instance details
            instances_detail = self.ecs_client.describe_container_instances(
                cluster=self.cluster_name,
                containerInstances=instance_arns
            )
            
            # Count tasks per instance
            task_distribution = {}
            total_tasks = 0
            
            for instance in instances_detail['containerInstances']:
                instance_id = instance['ec2InstanceId']
                running_tasks = instance['runningTasksCount']
                task_distribution[instance_id] = {
                    'running_tasks': running_tasks,
                    'remaining_cpu': instance['remainingResources'][0]['integerValue'] 
                        if instance['remainingResources'] else 0,
                    'remaining_memory': instance['remainingResources'][1]['integerValue']
                        if len(instance['remainingResources']) > 1 else 0,
                    'status': instance['status'],
                    'agent_connected': instance['agentConnected']
                }
                total_tasks += running_tasks
            
            # Calculate distribution metrics
            avg_tasks = total_tasks / len(instance_arns) if instance_arns else 0
            max_tasks = max(td['running_tasks'] for td in task_distribution.values())
            min_tasks = min(td['running_tasks'] for td in task_distribution.values())
            
            # Check if distribution is balanced (within 30% variance)
            is_balanced = (max_tasks - min_tasks) <= (avg_tasks * 0.3) if avg_tasks > 0 else True
            
            return {
                'total_instances': len(instance_arns),
                'total_tasks': total_tasks,
                'average_tasks_per_instance': avg_tasks,
                'max_tasks_on_instance': max_tasks,
                'min_tasks_on_instance': min_tasks,
                'is_balanced': is_balanced,
                'distribution': task_distribution
            }
            
        except ClientError as e:
            return {'error': str(e)}
    
    def verify_all_services_healthy(self) -> Tuple[bool, Dict]:
        """Comprehensive verification that all services are healthy"""
        try:
            results = {
                'timestamp': datetime.now().isoformat(),
                'cluster': self.cluster_name,
                'services': []
            }
            
            # List all services
            service_arns = []
            paginator = self.ecs_client.get_paginator('list_services')
            for page in paginator.paginate(cluster=self.cluster_name):
                service_arns.extend(page.get('serviceArns', []))
            
            if not service_arns:
                return True, results
            
            # Check each service
            services_response = self.ecs_client.describe_services(
                cluster=self.cluster_name,
                services=service_arns
            )
            
            all_healthy = True
            
            for service in services_response['services']:
                service_health = {
                    'name': service['serviceName'],
                    'desired_count': service['desiredCount'],
                    'running_count': service['runningCount'],
                    'pending_count': service['pendingCount'],
                    'deployment_count': len(service['deployments']),
                    'is_healthy': service['runningCount'] == service['desiredCount'] and service['pendingCount'] == 0
                }
                
                if not service_health['is_healthy']:
                    all_healthy = False
                
                results['services'].append(service_health)
            
            # Add task distribution info
            distribution = self.verify_task_distribution()
            results['task_distribution'] = distribution
            
            return all_healthy, results
            
        except ClientError as e:
            return False, {'error': str(e)}


# Example usage
def example_enhanced_reroll():
    """Example of using enhanced re-roll with container restart management"""
    
    from ecs_reroll_orchestrator import RerollConfig, AWSSessionManager
    
    # Configuration
    config = RerollConfig(
        cluster_name="my-ecs-cluster",
        asg_name="my-asg",
        new_ami_id="ami-0123456789abcdef0",
        role_arn="arn:aws:iam::123456789012:role/my-role",
        region="us-east-1"
    )
    
    # Recovery configuration
    recovery_config = ServiceRecoveryConfig(
        wait_for_service_stability=True,
        service_stability_timeout_minutes=15,
        force_new_deployment=True,  # Force services to redeploy tasks
        scale_up_percentage=20  # Temporarily scale up by 20% during migration
    )
    
    # Initialize orchestrator
    orchestrator = EnhancedECSRerollOrchestrator(config, recovery_config)
    orchestrator.session_manager = AWSSessionManager(config)
    
    # Perform complete re-roll with container management
    operation_id = orchestrator.perform_complete_reroll()
    
    print(f"Re-roll operation completed: {operation_id}")
    
    # Verify services are healthy
    verifier = ServiceVerification(config.cluster_name, config.region)
    is_healthy, verification_results = verifier.verify_all_services_healthy()
    
    print(f"All services healthy: {is_healthy}")
    print(json.dumps(verification_results, indent=2))
    
    return operation_id, is_healthy


if __name__ == "__main__":
    example_enhanced_reroll()

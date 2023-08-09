"""Botree AWS EC2 utilities."""

from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import List


class EC2:
    """AWS EC2 operations."""

    def __init__(self, session):
        """
        EC2 class init.

        Parameters
        ----------
        session : boto3.Session
            The authenticated session to be used for EC2 operations.
        """
        self.session = session
        self.ec2_client = self.session.client(service_name="ec2")
        self.cw_client = self.session.client(service_name="cloudwatch")

    def get_instance_id(self, instance_name=None) -> List[str]:
        """
        Retrieve the instance IDs associated with the given instance name.

        Parameters
        ----------
        instance_name : str, optional
            The name of the instance to retrieve IDs for.

        Returns
        -------
        List[str]
            A list of instance IDs.
        """
        filters = []

        if instance_name:
            filters.append({"Name": "tag:Name", "Values": [instance_name]})

        instances = self.ec2_client.describe_instances(Filters=filters)

        instance_ids = []

        for reservation in instances["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]
                instance_ids.append(instance_id)

        return instance_ids

    def stop_instance(self, instance_name: str):
        """
        Stop the EC2 instance with the specified instance name.

        Parameters
        ----------
        instance_name : str
            The name of the instance to stop.

        Raises
        ------
        Exception
            If the instance is already stopped or stopping, or if the instance cannot be stopped.
        """
        instance_id = self.get_instance_id(instance_name)[0]

        response = self.ec2_client.stop_instances(InstanceIds=[instance_id])
        status = response["StoppingInstances"][0]["CurrentState"]["Name"]
        if status == "stopped" or status == "stopping":
            raise Exception(f"Instance {instance_name} is already {status}.")
        else:
            raise Exception(f"Failed to stop instance {instance_name}")

    def start_instance(self, instance_name: str):
        """
        Start the EC2 instance with the specified instance name.

        Parameters
        ----------
        instance_name : str
            The name of the instance to start.

        Raises
        ------
        Exception
            If the instance is already running, or if the instance cannot be started.
        """
        instance_id = self.get_instance_id(instance_name)[0]

        response = self.ec2_client.start_instances(InstanceIds=[instance_id])
        status = response["StartingInstances"][0]["CurrentState"]["Name"]
        if status == "running":
            raise Exception(f"Instance {instance_name} is already running.")
        elif status != "pending":
            raise Exception(f"Failed to start instance {instance_name}.")

    def get_instance_status(self, instance_name=None) -> List[Dict[str, str]]:
        """
        Retrieve status information for EC2 instances.

        Parameters
        ----------
        instance_name : str, optional
            The name of the instance to retrieve status for.

        Returns
        -------
        List[Dict[str, str]]
            A list of dictionaries containing instance name, ID, and status information.
        """
        filters = []

        if instance_name:
            filters.append({"Name": "tag:Name", "Values": [instance_name]})

        instances = self.ec2_client.describe_instances(Filters=filters)
        instance_statuses = []

        for reservation in instances["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]
                instance_name = next(
                    (
                        tag["Value"]
                        for tag in instance.get("Tags", [])
                        if tag["Key"] == "Name"
                    ),
                    "N/A",
                )
                instance_status = instance["State"]["Name"]
                instance_statuses.append(
                    {
                        "instance_name": instance_name,
                        "instance_id": instance_id,
                        "instance_status": instance_status,
                    }
                )

        return instance_statuses

    def get_instance_cpu_usage(self, instance_name=None):
        """
        Retrieve average CPU utilization for EC2 instances of the last 5 minutes.

        Parameters
        ----------
        instance_name : str, optional
            The name of the instance to retrieve CPU utilization for.

        Returns
        -------
        List[Dict[str, Union[str, float]]]
            A list of dictionaries containing instance name, ID, and average CPU utilization of the last 5 minutes.
        """
        filters = []

        if instance_name:
            filters.append({"Name": "tag:Name", "Values": [instance_name]})

        instances = self.ec2_client.describe_instances(Filters=filters)
        instance_statuses = []

        for reservation in instances["Reservations"]:
            for instance in reservation.get("Instances", []):
                instance_id = instance["InstanceId"]

                response = self.cw_client.get_metric_statistics(
                    Namespace="AWS/EC2",
                    MetricName="CPUUtilization",
                    Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                    StartTime=datetime.utcnow() - timedelta(minutes=5),
                    EndTime=datetime.utcnow(),
                    Period=300,
                    Statistics=["Average"],
                )

                datapoints = response.get("Datapoints", [])
                average_cpu_utilization = (
                    round(
                        sum(point.get("Average", 0) for point in datapoints)
                        / len(datapoints),
                        2,
                    )
                    if datapoints
                    else 0
                )

                instance_name = next(
                    (
                        tag["Value"]
                        for tag in instance.get("Tags", [])
                        if tag["Key"] == "Name"
                    ),
                    "N/A",
                )

                instance_statuses.append(
                    {
                        "instance_name": instance_name,
                        "instance_id": instance_id,
                        "average_cpu_utilization": average_cpu_utilization,
                    }
                )

        if instance_name and not instance_statuses:
            raise Exception(f"No instances found with the name {instance_name}.")

        return instance_statuses

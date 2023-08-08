"""Botree AWS EC2 utilities."""

from typing import List
from typing import Dict


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
        self.client = self.session.client(service_name="ec2")

    def _get_instance_id(self, instance_name=None) -> List[str]:
        filters = []

        if instance_name:
            filters.append({"Name": "tag:Name", "Values": [instance_name]})

        instances = self.client.describe_instances(Filters=filters)

        instance_ids = []

        for reservation in instances["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]
                instance_ids.append(instance_id)

        return instance_ids

    def stop_instance(self, instance_name: str):
        instance_id = self._get_instance_id(instance_name)[0]

        response = self.client.stop_instances(InstanceIds=[instance_id])
        status = response["StoppingInstances"][0]["CurrentState"]["Name"]
        if status == "stopped" or status == "stopping":
            raise Exception(f"Instance {instance_name} is already {status}.")
        else:
            raise Exception(f"Failed to stop instance {instance_name}")

    def start_instance(self, instance_name: str):
        instance_id = self._get_instance_id(instance_name)[0]

        response = self.client.start_instances(InstanceIds=[instance_id])
        status = response["StartingInstances"][0]["CurrentState"]["Name"]
        if status == "running":
            raise Exception(f"Instance {instance_name} is already running.")
        elif status != "pending":
            raise Exception(f"Failed to start instance {instance_name}.")


import boto3
import dataiku
import dku_emr
import logging
import time
from dataiku.runnables import Runnable

# This actually belongs in the main entry point
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        dss_cluster = dataiku.api_client().get_cluster(self.config["dss_cluster_id"])
        settings = dss_cluster.get_settings()
        clusterConfig = settings.get_raw()["params"]["config"]
        (client, emr_cluster_id) = dku_emr.get_client_and_wait(settings)

        logging.info("retrieving current instances")
        response = client.list_instance_groups(ClusterId=emr_cluster_id)

        core_group = None
        task_group = None
        for group in response["InstanceGroups"]:
            if group["InstanceGroupType"] == "CORE":
                if not core_group:
                    core_group = group
                else:
                    raise Exception("Configuration not supported: multiple CORE groups: %s" % response["InstanceGroups"])
            elif group["InstanceGroupType"] == "TASK":
                if not task_group:
                    task_group = group
                else:
                    raise Exception("Configuration not supported: multiple TASK groups: %s" % response["InstanceGroups"])

        logging.info("Current instance groups: core=%s task=%s" % (core_group, task_group))

        instanceGroupsToAdd = []
        instanceGroupsToModify = []

        if core_group and core_group["RequestedInstanceCount"] != self.config.get("core_group_target_instances", 0):
            instanceGroupsToModify.append({
                    "InstanceGroupId" : core_group["Id"],
                    "InstanceCount" : self.config.get("core_group_target_instances", 0)
                })
        elif not core_group and self.config.get("core_group_target_instances"):
            if not clusterConfig.get("coreInstanceType"):
                raise Exception("Missing core instance type in cluster config")
            instanceGroupsToAdd.append({
                    'InstanceRole': 'CORE',
                    'InstanceType': clusterConfig["coreInstanceType"],
                    'InstanceCount': self.config["core_group_target_instances"]
                })

        if task_group and task_group["RequestedInstanceCount"] != self.config.get("task_group_target_instances", 0):
            instanceGroupsToModify.append({
                    "InstanceGroupId" : task_group["Id"],
                    "InstanceCount" : self.config.get("task_group_target_instances", 0)
                })
        elif not task_group and self.config.get("task_group_target_instances"):
            if not clusterConfig.get("taskInstanceType"):
                raise Exception("Missing task instance type in cluster config")
            instanceGroupsToAdd.append({
                    'InstanceRole': 'TASK',
                    'InstanceType': clusterConfig["taskInstanceType"],
                    'InstanceCount': self.config["task_group_target_instances"]
                })

        if instanceGroupsToAdd:
            logging.info("Adding new instance groups: %s" % instanceGroupsToAdd)
            client.add_instance_groups(InstanceGroups=instanceGroupsToAdd, JobFlowId=emr_cluster_id)

        if instanceGroupsToModify:
            logging.info("Modifying current instance groups: %s" % instanceGroupsToModify)
            client.modify_instance_groups(ClusterId=emr_cluster_id, InstanceGroups=instanceGroupsToModify)

        if self.config.get("wait_for_completion", False):
            # The transition RUNNING -> RESIZING is not synchronous
            time.sleep(60)
            i = 0
            while True:
                response = client.list_instance_groups(ClusterId=emr_cluster_id)
                done = True
                for group in response["InstanceGroups"]:
                    instanceGroupType = group["InstanceGroupType"]
                    state = group["Status"]["State"]
                    if instanceGroupType == "CORE" or instanceGroupType == "TASK":
                        # TODO check for error states?
                        if state == "PROVISIONING" or state == "RESIZING":
                            done = False
                if done:
                    break
                elif i == 60:
                    raise Exception("timeout waiting for resize operation to complete: %s" % response["InstanceGroups"])
                else:
                    logging.info("Waiting for resize operation to complete")
                    i = i + 1
                    time.sleep(30)

        return "Done"

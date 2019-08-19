import dataiku, boto3, logging, dku_emr
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
        (client, emr_cluster_id) = dku_emr.get_client_and_wait(settings)

        logging.info("retrieving instance groups")
        instance_groups = client.list_instance_groups(ClusterId=emr_cluster_id)

        logging.info("retrieving master instance")
        master_instances = client.list_instances(ClusterId=emr_cluster_id,
                InstanceGroupTypes=['MASTER'],
                InstanceStates=['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING', 'RUNNING'])
        master_instance_info = {"privateIpAddress" : master_instances['Instances'][0]["PrivateIpAddress"]}

        logging.info("retrieving slave instances")
        slave_instances = client.list_instances(ClusterId=emr_cluster_id,
            InstanceGroupTypes=['CORE', 'TASK'],
            InstanceStates=['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING', 'RUNNING'])
        slave_instances_info = [
                {"privateIpAddress":  inst["PrivateIpAddress"]} for inst in slave_instances["Instances"]]

        return {
            "masterInstance" : master_instance_info,
            "slaveInstances":  slave_instances_info,
            'instanceGroups':[
                    {"instanceGroupId" : x["Id"],
                     "runningInstanceCount": x["RunningInstanceCount"],
                     "instanceType" : x["InstanceType"],
                     "instanceGroupType" : x["InstanceGroupType"],
                     "status": x["Status"]["State"]
                    } for x in instance_groups["InstanceGroups"]]
               }
        
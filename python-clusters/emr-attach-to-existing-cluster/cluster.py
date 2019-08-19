import boto3
import os, json, logging
import dku_emr
from dataiku.cluster import Cluster

# This actually belongs in the main entry point
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyCluster(Cluster):
    def __init__(self, cluster_id, cluster_name, config, plugin_config):
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.config = config
        self.plugin_config = plugin_config
       
    def start(self):
        region_name = self.config.get("awsRegionId") or dku_emr.get_current_region()
        client = boto3.client("emr", region_name=region_name)
        clusterId = self.config["emrClusterId"]
        logging.info("Attaching to EMR cluster id %s" % clusterId)
        return dku_emr.make_cluster_keys_and_data(client, clusterId, create_user_dir=True)

    def stop(self, data):
        """
        Since we attached to an existing cluster, we don't stop it
        """
        logging.info("Detaching: nothing to do")

import os
import logging

from dataiku.cluster import Cluster
from cluster_ops import ClusterBuilder, ClusterStopper

# This actually belongs in the main entry point
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


class MyCluster(Cluster):

    def __init__(self, cluster_id, cluster_name, config, plugin_config):
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.config = config
        self.plugin_config = plugin_config
        self.resource_dir = os.getenv('DKU_CUSTOM_RESOURCE_FOLDER')

    def start(self):
        return ClusterBuilder(my_cluster=self).build_cluster()

    def stop(self, data):
        """
        Stop the cluster
        :param data: the dict of data that the start() method produced for the cluster
        """
        return ClusterStopper(my_cluster=self).terminate_cluster(data)

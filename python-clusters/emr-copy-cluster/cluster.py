import os
import logging
from dataiku.cluster import Cluster

from cluster_ops import ClusterCopier, ClusterStopper

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
        self.excluded_cluster_types = [x.strip() for x in self.config.get('excluded_cluster_types').split(",")]
        self.source_cluster_id = self.config.get('source_dss_cluster_id')
        self.source_cluster_name = self.source_cluster_id
        
    def start(self):
        return ClusterCopier(self).copy_cluster()

    def stop(self, data):
        return ClusterStopper(self).terminate_cluster(data)
    

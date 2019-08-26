import logging
from dataiku.cluster import Cluster
from cluster_ops import ClusterAttacher, ClusterStopper

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
        return ClusterAttacher(my_cluster=self).attach_cluster()

    def stop(self, data):
        """
        Since we attached to an existing cluster, we don't stop it
        """
        return ClusterStopper(my_cluster=self).detach_cluster(data)

import boto3
import os
import logging
import json
import subprocess

import dku_emr
import dataiku
from dataiku.cluster import Cluster
from boto_params import Arg, Constant, Response
from ssh.client import RemoteSSHClient

# This actually belongs in the main entry point
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


class ClusterBuilder(object):

    def __init__(self, my_cluster):
        self.my_cluster = my_cluster
        
    def build_cluster(self):
        region = self.my_cluster.config.get("awsRegionId") or dku_emr.get_current_region()
        client = boto3.client('emr', region_name=region)
        release = 'emr-%s' % self.my_cluster.config["emrVersion"]
        name_prefix = "dss-"
        name = (
            name_prefix + self.my_cluster.cluster_id 
            if self.my_cluster.cluster_id[0:len(name_prefix)] != name_prefix 
            else self.my_cluster.cluster_id
        )
        logging.info("starting cluster, release=%s name=%s" % (release, name))

        extra_args = {}

        # Path for logs in S3
        logs_path = self.my_cluster.config.get("logsPath")
        if logs_path is not None:
            if "s3://" in logs_path:
                extra_args[Arg.LogUri] = logs_path
            else:
                raise Exception("'{}' is not a valid S3 path".format(logs_path))

        # Use specified security config
        if "securityConfiguration" in self.my_cluster.config:
            extra_args[Arg.SecurityConfig] = self.my_cluster.config["securityConfiguration"]

        # EBS root volume size (minimum of 10)
        extra_args[Arg.EbsRootVolSize] = int(self.my_cluster.config.get('ebsRootVolumeSize') or 25) 
        
        # EMR app (e.g., Spark, Hive) configs
        extra_args[Arg.Configurations] = self._get_software_configs()

        # EMR instance groups and configs
        instances = self._get_instance_configs()

        # EMR applications to install
        applications = self._get_apps_to_install()

        # Tags
        tags = self._get_tags(name)

        # All args to run_job_flow(..)
        job_flow_params = {
            Arg.Name: name,
            Arg.Release: release,
            Arg.Instances: instances,
            Arg.Applications: applications,
            Arg.VisibleToAllUsers: True,
            Arg.JobFlowRole: self.my_cluster.config["nodesRole"],
            Arg.ServiceRole: self.my_cluster.config["serviceRole"],
            Arg.Tags: tags
        }
        job_flow_params.update(extra_args)

        logging.info("Starting cluster: %s", job_flow_params)

        response = client.run_job_flow(**job_flow_params)
        cluster_id = response[Response.JobFlowId]
        logging.info("cluster_id=%s" % cluster_id)

        logging.info("waiting for cluster to start")
        client.get_waiter('cluster_running').wait(ClusterId=cluster_id)

        cluster_keys_and_data = dku_emr.make_cluster_keys_and_data(
            client, cluster_id, create_user_dir=True, create_databases=self.my_cluster.config.get("databasesToCreate")
        )
        
        # TODO: Implement install of python libs and post-launch SSH commands to run on each node
        # try:
        #     python_libs = self.my_cluster.config.get('pythonLibs') or self.my_cluster.config.get('python_libs')
        #     if str(python_libs) not in {"None", "[]"}:
        #         self._install_python_libs()
        # 
        #     extra_setup_cmds = self.my_cluster.config.get('extraSetup') or self.my_cluster.config.get('extra_setup')
        #     if str(extra_setup_cmds) not in {"None", "[]"}:
        #         self._run_shell_commands()
        #         
        #     return cluster_keys_and_data
        # 
        # except Exception as e:
        #     logging.info(e)
        #     logging.info("Setup failed. Terminating cluster.")
        #     self.my_cluster.stop(data={'emrClusterId': cluster_id})
        return cluster_keys_and_data

    def _get_instance_configs(self):
        subnet = self.my_cluster.config.get("subnetId") or dku_emr.get_current_subnet()
        security_groups = self._get_security_groups()
        instances = {
            Arg.InstanceGroups: [self._get_master_group_config()],
            Arg.KeepJobFlowAliveWhenNoSteps: True,
            Arg.Ec2SubnetId: subnet,
            Arg.AddlMasterSecurityGroups: security_groups,
            Arg.AddlSlaveSecurityGroups: security_groups,
            Arg.TerminationProtected: (self.my_cluster.config.get('terminationProtected') or False)
        }

        # Need >= 1 core nodes
        if not self.my_cluster.config.get("coreInstanceType"):
            raise Exception("Missing core instance type")
        instances[Arg.InstanceGroups].append(self._get_slave_group_config(Constant.Core.lower()))

        # Don't need task group instance type unless requested count >= 1
        if self.my_cluster.config.get("taskInstanceCount"):
            if not self.my_cluster.config.get("taskInstanceType"):
                raise Exception("Missing task instance type")
            instances[Arg.InstanceGroups].append(self._get_slave_group_config(Constant.Task.lower()))

        ec2_keyname = self._get_ec2_keyname()
        if ec2_keyname:
            instances[Arg.Ec2KeyName] = ec2_keyname

        return instances

    def _get_ec2_keyname(self):
        """Use user-specified keypair, o/w the one specified (if any) in default settings"""
        ec2_keyname = self.my_cluster.config.get('ec2KeyName')

        return ec2_keyname if ec2_keyname != "" else None

    def _get_master_group_config(self):
        return {
            Arg.InstanceRole: Constant.Master,
            Arg.Market: Constant.OnDemand,
            Arg.InstanceType: self.my_cluster.config["masterInstanceType"],
            Arg.InstanceCount: 1,
            Arg.EbsConfig: {
                Arg.EbsBlockDeviceConfigs: [
                    {
                        Arg.EbsVolSpec: {
                            Arg.EbsVolType: Constant.Gp2,
                            # Arg.Iops: 123,
                            Arg.EbsVolSizeGb: self.my_cluster.config.get('masterEbsSize') or defaults['size_gb']
                        },
                        Arg.EbsVolsPerInstance: self.my_cluster.config.get('masterEbsCount') or defaults['count']
                    },
                ],
                Arg.EbsOptimized: self.my_cluster.config.get('ebsOptimized') or True
            },
        }

    def _get_slave_group_config(self, group_id):
        core = Constant.Core.lower()
        task = Constant.Task.lower()
        if group_id not in {core, task}:
            raise Exception(
                "Unknown instance group type. Must be either '{}' or '{}'.".format(core, task)
            )

        if self.my_cluster.config.get('{}InstanceCount'.format(group_id)) == 0:
            if group_id == core:
                raise Exception(
                    "0 {} instances requested. You must request at least 1.".format(Constant.Core.upper())
                )
            return None

        instance_group = {
            Arg.InstanceRole: group_id.upper(),
            Arg.InstanceType: self.my_cluster.config['{}InstanceType'.format(group_id)],
            Arg.InstanceCount: self.my_cluster.config['{}InstanceCount'.format(group_id)],
            Arg.EbsConfig: {
                Arg.EbsBlockDeviceConfigs: [
                    {
                        Arg.EbsVolSpec: {
                            Arg.EbsVolType: Constant.Gp2,
                            # Arg.Iops: 123,
                            Arg.EbsVolSizeGb: self.my_cluster.config.get('{}EbsSize'.format(group_id))
                        },
                        Arg.EbsVolsPerInstance: self.my_cluster.config.get('{}EbsCount'.format(group_id))
                    },
                ],
                Arg.EbsOptimized: self.my_cluster.config.get('ebsOptimized') or True
            }
        }
        market = Constant.OnDemand
        if self.my_cluster.config.get('{}UseSpotInstances'.format(group_id)):
            market = Constant.Spot
            bid_price = self.my_cluster.config.get('{}BidPrice'.format(group_id)) or None
            if bid_price:
                instance_group[Arg.BidPrice] = bid_price

        instance_group[Arg.Market] = market

        return instance_group

    def _get_software_configs(self):
        hive_props = {}

        if self.my_cluster.config["metastoreDBMode"] == "CUSTOM_JDBC":
            hive_props = {
                "javax.jdo.option.ConnectionURL": self.my_cluster.config["metastoreJDBCURL"],
                "javax.jdo.option.ConnectionDriverName": self.my_cluster.config["metastoreJDBCDriver"],
                "javax.jdo.option.ConnectionUserName": self.my_cluster.config["metastoreJDBCUser"],
                "javax.jdo.option.ConnectionPassword": self.my_cluster.config["metastoreJDBCPassword"],
            }
        elif self.my_cluster.config["metastoreDBMode"] == "MYSQL":
            hive_props = {
                "javax.jdo.option.ConnectionURL": "jdbc:mysql://{}:3306/hive?createDatabaseIfNotExist=true".format(
                    self.my_cluster.config["metastoreMySQLHost"]
                ),
                "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                "javax.jdo.option.ConnectionUserName": self.my_cluster.config["metastoreMySQLUser"],
                "javax.jdo.option.ConnectionPassword": self.my_cluster.config["metastoreMySQLPassword"]
            }
        elif self.my_cluster.config["metastoreDBMode"] == "AWS_GLUE_DATA_CATALOG":
            hive_props = {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }

        configurations = self.my_cluster.config.get('softwareConfig') or []
        if not isinstance(configurations, list):
            configurations = json.JSONDecoder().decode(configurations)
        hive_site_config = ClusterBuilder._get_app_config(config_id="hive-site", configurations=configurations)
        if hive_site_config is not None:
            hive_site_config['Properties'].update(hive_props)
        else:
            configurations.append(
                {
                    Arg.Classification: "hive-site",
                    Arg.Properties: hive_props
                }
            )

        self.my_cluster.config['softwareConfig'] = json.JSONEncoder().encode(configurations)

        return configurations

    @staticmethod
    def _get_app_config(config_id, configurations):
        app_config_search = [c for c in configurations if c[Arg.Classification] == config_id]

        return app_config_search[0] if len(app_config_search) > 0 else None

    def _get_apps_to_install(self):
        app_install_key_pattern = "installapp"
        app_install_keys = [
            {
                'Name': key.lower().split(app_install_key_pattern)[-1].capitalize()
            } for key in self.my_cluster.config.keys() 
            if self.my_cluster.config.get(key) and len(key.lower().split(app_install_key_pattern)) > 1
        ]

        return app_install_keys if len(app_install_keys) > 0 else []

    def _get_security_groups(self):
        if self.my_cluster.config.get("additionalSecurityGroups"):
            return [
                x.strip() for x in self.my_cluster.config.get("additionalSecurityGroups").split(",")
            ]
        
        return []

    def _get_tags(self, name):
        tags = [
            {
                Arg.Key: "Name",
                Arg.Value: name
            }
        ]
        for tag in (self.my_cluster.config.get("tags") or []):
            if tag[Arg.Key] == "Name":
                next(
                    (
                        v for i, v in enumerate(tags) if v[Arg.Key] == "Name"
                    )
                ).update({Arg.Key: tag[Arg.Key], Arg.Value: tag[Arg.Value]})
                continue

            tags.append(
                {
                    Arg.Key: tag[Arg.Key],
                    Arg.Value: tag[Arg.Value]
                }
            )

        return tags

    # TODO: Implement
    def _install_python_libs(self):
        raise NotImplementedError("To be implemented...")
    
    # TODO: Implement
    def _run_shell_commands(self):
        raise NotImplementedError("To be implemented...")
        
        
class ClusterStopper(object):
    def __init__(self, my_cluster):
        self.my_cluster = my_cluster
        
    def terminate_cluster(self, data):
        emr_cluster_id = data["emrClusterId"]
        region = self.my_cluster.config.get("awsRegionId") or dku_emr.get_current_region()
        client = boto3.client('emr', region_name=region)

        # Make sure termination protection is turned off
        client.set_termination_protection(JobFlowIds=[emr_cluster_id], TerminationProtected=False)
        client.terminate_job_flows(JobFlowIds=[emr_cluster_id])
        
    def detach_cluster(self, data):
        """
        Since we attached to an existing cluster, we don't stop it
        """
        msg = "Detaching. Nothing to do."
        logging.info(msg)

        return msg


class ClusterAttacher(object):
    
    def __init__(self, my_cluster):
        self.my_cluster = my_cluster
        
    def attach_cluster(self):
        region_name = self.my_cluster.config.get("awsRegionId") or dku_emr.get_current_region()
        client = boto3.client("emr", region_name=region_name)
        cluster_id = self.my_cluster.config["emrClusterId"]
        logging.info("Attaching to EMR cluster id %s" % cluster_id)

        return dku_emr.make_cluster_keys_and_data(client, cluster_id, create_user_dir=True)
    

class ClusterCopier(object):
    
    def __init__(self, my_cluster):
        self.my_cluster = my_cluster
        
    def copy_cluster(self):
        dss_client = dataiku.api_client()
        existing_clusters = dss_client.list_clusters()
        cluster_id_search = [c['id'] for c in existing_clusters if c['id'] == self.my_cluster.source_cluster_id]
        
        if len(cluster_id_search) == 0:
            raise Exception("Cluster id '{}' does not exist.".format(self.my_cluster.source_cluster_id))

        template_cluster_settings = dss_client.get_cluster(cluster_id_search[0]).get_settings().settings
        if template_cluster_settings['type'] in self.my_cluster.excluded_cluster_types:
            raise Exception("Cannot copy from cluster of type '{}'.".format(template_cluster_settings['type']))
        
        # Use cluster.json file to specify settings new new cluster
        params_template_path = os.path.join(
            "/".join(self.my_cluster.resource_dir.split("/")[0:-1]),
            "python-clusters/emr-create-cluster"
        )
        # Copy settings from source/template cluster to apply to new one
        params_template = json.load(open(os.path.join(params_template_path, "cluster.json"), "r"))
        params_to_copy = template_cluster_settings['params']['config']
        for key, val in params_to_copy.items():
            params_template[key] = val

        self.my_cluster.config = params_template
        
        logging.info("Building new cluster '{}' using '{}' as a template".format(
            self.my_cluster.cluster_id, self.my_cluster.source_cluster_id)
        )
        
        return ClusterBuilder(self.my_cluster).build_cluster()
        
        
import boto3
import paramiko
import sys
import subprocess
from time import sleep
from numpy import unique
import dataiku
import logging

from client import RemoteSSHClient


def get_emr_cluster_info(dss_cluster_id):
    """Returns dictionary containing info about the EMR cluster used by a project."""
    # client = client or dataiku.api_client()
    # cluster_info_macro = proj.get_macro("pyrunnable_emr-clusters_get-cluster-info")
    # dss_cluster_info = {'dss_cluster_id': dss_cluster_id}
    # response = cluster_info_macro.get_result(cluster_info_macro.run(params=dss_cluster_info))
    
    cluster = dataiku.api_client().get_cluster(dss_cluster_id)
    cluster_settings = cluster.get_settings().settings
    emr_cluster_id = cluster_settings['data']['emrClusterId']
    aws_region_id = cluster_settings['params']['config']['awsRegionId']
    
    boto_client = boto3.client('emr', region_name=aws_region_id)
    instance_groups = {
        g['Id']: {} 
        for g in boto_client.list_instance_groups(ClusterId=emr_cluster_id)
    }
    for gid in instance_groups.keys():
        pass
    
    dss_cluster_info = {
        'dss_cluster_id': dss_cluster_id,
        'emr_cluster_id': emr_cluster_id,
        'instance_groups': instance_groups
    }

    logging.info("retrieving master instance")
    master_instances = boto_client.list_instances(ClusterId=emr_cluster_id,
                                             InstanceGroupTypes=['MASTER'],
                                             InstanceStates=['AWAITING_FULFILLMENT', 'PROVISIONING', 'BOOTSTRAPPING',
                                                             'RUNNING'])
    master_instance_info = {"privateIpAddress": master_instances['Instances'][0]["PublicIpAddress"]}

    # if cluster_mgr_project_key is not None:
    #     cluster_mgr_info = client.get_project(cluster_mgr_project_key).get_variables(
    #     )['standard']['emr']['clusters'].get(dss_cluster_id)
    #     if cluster_mgr_info is not None:
    #         instance_groups = cluster_mgr_info['instance_groups']
    #         for dss_grp_info in dss_cluster_info['instanceGroups']:
    #             grp_id = dss_grp_info['instanceGroupId']
    #             cluster_mgr_grp_info = instance_groups.get(grp_id)
    #             if cluster_mgr_grp_info:
    #                 dss_grp_info.update(
    #                     {'resizable': cluster_mgr_grp_info.get('resizable', False)}
    #                 )

    return dss_cluster_info


def cleanup(ip_to_connectionvals_dict, scen_vars, all_exit_zero_status):
    print("Closing all channels and connections ...")
    for v in ip_to_connectionvals_dict.values():
        v['client'].close()
        '''
        for w in v.values():
            w['stdin'].channel.close()
            w['stdout'].channel.close()
            w['stderr'].channel.close()
        '''
    abort_cond = bool(scen_vars['abort_on_nonzero_exit'])
    # if abort_cond and all_exit_zero_status != 0:
    #     print("\n WARNING: NOT ALL REMOTE PROCESSES RETURNED EXIT CODE 0. ABORTING SCENARIO.\n")
    #     DSSScenario(
    #         api_client(),
    #         "DSS_ADMIN",
    #         "INSTALL_ON_EMR_NODES"
    #     ).abort()


def run_install_commands(scen_vars, cluster_id, instance_group_ids):
    dss_id = scen_vars["dss_instance_id"]
    username = scen_vars['username']
    keyfile_path = scen_vars['keyfile_path']
    delete_keyfile = False
    # if "://" in keyfile_path:
    #     keyfile_path, delete_keyfile = helpers.download_keyfile(keyfile_path)

    key = paramiko.RSAKey.from_private_key_file(keyfile_path)
    ssh_cmd_list = scen_vars['ssh_cmd_list']
    all_exit_zero_status = 0
    ip_to_sshvals_dict = {}

    print("GOING TO RUN FOLLOWING COMMANDS:", ssh_cmd_list)

    try:
        emr = boto3.client(
            'emr',
            aws_access_key_id=scen_vars['s3_access_key'],
            aws_secret_access_key=scen_vars['s3_secret_key'],
            region_name=scen_vars['aws_region']
        )
        paginator = emr.get_paginator('list_instances')

        sleep_interval = float(scen_vars['sleep_interval'])
        for gid in instance_group_ids:
            page_iterator = paginator.paginate(ClusterId=cluster_id, InstanceGroupId=gid, InstanceStates=['RUNNING'])
            node_ips = []
            for p in page_iterator:
                node_ips += [x['PublicIpAddress'] for x in p['Instances']]
            for ip in node_ips:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(hostname=ip, username=username, pkey=key)
                client.invoke_shell()
                ip_to_sshvals_dict[ip] = {}
                ip_to_sshvals_dict[ip]['client'] = client
                for cmd in ssh_cmd_list:
                    stdin, stdout, stderr = client.exec_command(cmd)
                    sleep(sleep_interval)
                    ip_to_sshvals_dict[ip][cmd] = {}
                    ip_to_sshvals_dict[ip][cmd]['stdin'] = stdin
                    ip_to_sshvals_dict[ip][cmd]['stdout'] = stdout
                    ip_to_sshvals_dict[ip][cmd]['stderr'] = stderr
                    ip_to_sshvals_dict[ip][cmd]['exit_status'] = -1
                    ip_to_sshvals_dict[ip][cmd]['is_done'] = False

        all_done = False
        while not all_done:
            all_done = True
            for ip, v in ip_to_sshvals_dict.items():
                query_items = {k: v[k] for k in v.keys() if k != 'client'}
                for cmd, w in query_items.items():
                    if not w['is_done']:
                        if w['stdout'].channel.exit_status_ready():
                            w['exit_status'] = w['stdout'].channel.recv_exit_status()
                            w['is_done'] = True
                            print("\nProcess '%s' on node %s returned exit status %s\n" % (cmd, ip, w['exit_status']))
                            all_exit_zero_status = max(all_exit_zero_status, abs(w['exit_status']))
                            continue
                        all_done = False

        print("\nAll processes complete.\n")

        process_exit_statuses = []
        for ip in ip_to_sshvals_dict.keys():
            process_exit_statuses += [
                int(r['exit_status']) for cmd, r in ip_to_sshvals_dict[ip].items() \
                if cmd != "client"
            ]
        process_exit_statuses = unique(process_exit_statuses)
        print(
            "COMMAND EXIT STATUS SUMMARY --> All successful: {}\n".format(
                True if (len(process_exit_statuses) == 1 and process_exit_statuses[0] == 0) \
                    else False
            )
        )

        for ip in ip_to_sshvals_dict.keys():
            process_exit_statuses = unique(
                [
                    int(r['exit_status']) for cmd, r in ip_to_sshvals_dict[ip].items() \
                    if cmd != "client"
                ]
            )
            print(
                "Node {} --> All successful: {}".format(
                    ip,
                    True if (len(process_exit_statuses) == 1 and process_exit_statuses[0] == 0) \
                        else False
                )
            )
            for cmd, results in ip_to_sshvals_dict[ip].items():
                if cmd == "client":
                    continue
                print("\t'{}': {}".format(cmd, results['exit_status']))
            print("\n")

    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

    finally:
        if delete_keyfile:
            subprocess.check_output("rm " + keyfile_path, shell=True)

        cleanup(ip_to_sshvals_dict, scen_vars, all_exit_zero_status)


def run():
    # scen = Scenario()
    # scen_vars = scen.get_all_variables()
    # project_key = scen_vars['projectKey']
    # project_vars = api_client().get_project(project_key).get_variables()['standard']
    # tier = scen_vars["dss_instance_type"]
    # emr_params = project_vars['emr'][tier]
    # cluster_id = scen_vars['cluster_id'] if 'cluster_id' in scen_vars else emr_params['cluster_id']
    # instance_group_ids = None
    # if 'instance_group_ids' in scen_vars:
    #     instance_group_ids = scen_vars['instance_group_ids'].split(",")
    # else:
    #     instance_group_ids = emr_params['task_instance_group_id'].split(",")
    # 
    # run_install_commands(scen_vars, cluster_id, instance_group_ids)
    pass

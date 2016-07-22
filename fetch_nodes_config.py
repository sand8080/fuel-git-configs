#    Copyright 2016 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import argparse
import datetime
import os
import sys
import yaml

import git
import paramiko
import scp


def get_nodes_info(config_file):
    print("Fetching nodes info from: {0}".format(config_file))
    with open(config_file) as stream:
        content = yaml.load(stream)
        for node_info in content['network_metadata']['nodes'].itervalues():
            yield {
                'ip': node_info['network_roles']['admin/pxe'],
                'node_id': node_info['uid']
            }


def switch_to_branch(repo, branch_name):
    current_heads = [head.name for head in repo.heads]

    if branch_name != repo.active_branch:
        if branch_name not in current_heads:
            print("Creating branch {0} in repo {1}".format(
                branch_name, repo.working_dir))
            repo.git.checkout(None, b=branch_name)
        else:
            print("Switching to branch {0} in repo {1}".format(
                branch_name, repo.working_dir))
            repo.git.checkout(branch_name)
    print("Repo {0} active branch: {1}".format(
            repo.working_dir, repo.active_branch))


def get_remote_configs_list(ssh, src):
    _, stdout, _ = ssh.exec_command('ls {0}'.format(src))
    return stdout.read().splitlines()


def fetch_config(repo, node_info, cluster_id):
    node_id = node_info['node_id']
    node_ip = node_info['ip']
    print("Fetching node {0} configuration from {1}".format(
        node_id, node_ip))
    node_configs_dir = os.path.join(repo.working_dir, 'nodes', node_id)

    if not os.path.exists(node_configs_dir):
        os.makedirs(node_configs_dir)

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
    ssh.connect(node_ip)
    src = '/etc/fuel/cluster/{0}'.format(cluster_id)
    get_remote_configs_list(ssh, src)

    with scp.SCPClient(ssh.get_transport()) as scp_obj:
        for remote_config in get_remote_configs_list(ssh, src):
            scp_obj.get(
                os.path.join(src, remote_config),
                local_path=node_configs_dir
            )
        print("Configuration from {0}:{1} saved to {2}".format(
            node_ip, src, node_configs_dir))


def commit_configs(repo):
    repo.index.add(['nodes'])
    if repo.is_dirty():
        message = 'Updating configs on {0}'.format(
            datetime.datetime.now())
        repo.index.commit(message)
        print("Changes committed")
    else:
        print("No changes to be committed")


def put_configs_to_repo(config_file, destination, cluster_id):
    print("Fetching configs for env {0} to repo {1}".format(
        cluster_id, destination))

    branch_name = '{0}'.format(cluster_id)
    repo = git.Repo(destination)
    switch_to_branch(repo, branch_name)

    nodes_info = get_nodes_info(config_file)
    for node_info in nodes_info:
        fetch_config(repo, node_info, cluster_id)

    commit_configs(repo)


def execute(params):
    if not params.cluster:
        print("Cluster id not defined")
        sys.exit(1)
    config_file = os.path.join(params.config_dir,
                               '{0}'.format(params.cluster),
                               'astute.yaml')
    put_configs_to_repo(config_file, params.repo_path, params.cluster)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-b', '--config-dir',
        help="Path to clusters configs directory",
        default='/etc/fuel/cluster'
    )
    parser.add_argument(
        '-r', '--repo_path',
        help="Path to config storage",
        default='/etc/fuel/config_repo'
    )
    parser.add_argument(
        '-c', '--cluster',
        help="Cluster id",
        type=int
    )

    params = parser.parse_args()
    execute(params)

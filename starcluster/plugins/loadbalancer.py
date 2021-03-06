# Copyright 2016 Vasisht Tadigotla
#
# This file is a plugin for StarCluster.
#
# This plugin is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

from starcluster import clustersetup
from starcluster.logger import log
from starcluster import static
from starcluster import config
import tempfile


class LoadBalancer(clustersetup.DefaultClusterSetup):
    """Run loadbalancer on master

    Example config:

    [plugin loadbalancer]
    SETUP_CLASS = starcluster.plugins.loadbalancer.LoadBalancer
    min_nodes = 2
    max_nodes = 10
    kill_cluster = True
    starcluster = git://github.com/vasisht/StarCluster.git
    branch = devel
    options = -a 2 --spot-bid 1.0
    config_file = /home/vasisht/.starcluster/config

    max_nodes and min_nodes are required parameters.
    Additional options to loadbalance can be passed
    as a string to the options parameter. StarCluster
    needs to be installed on the master instance if
    the starcluster parameter is not specified.

    """

    def __init__(self, min_nodes=None, max_nodes=None, kill_cluster=None,
                 starcluster=None, branch=None, options=None,
                 config_file=None):
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes
        self.kill_cluster = str(kill_cluster).lower() == "true"
        self.starcluster = starcluster
        self.branch = branch
        self.options = options
        self.config_file = config_file

    def run(self, nodes, master, user, user_shell, volumes):
        if self.min_nodes is None:
            log.fatal("Missing required min_nodes argument")
        if self.max_nodes is None:
            log.fatal("Missing required max_nodes argument")
        if self.starcluster is None:
            log.warn("Missing starcluster argument")
        else:
            log.info('Installing StarCluster on master')
            if self.branch is None:
                self._install_starcluster(master, self.starcluster)
            else:
                self._install_starcluster(master, self.starcluster,
                                          self.branch)
        config_dir = '/root/.starcluster'
        remote_config = config_dir + '/config'
        if not master.ssh.path_exists(config_dir):
            master.ssh.mkdir(config_dir)
        log.info('Transferring StarCluster config')
        master.ssh.put(self.config_file, remote_config)
        cfg = config.StarClusterConfig().load()
        aws_key = cfg.keys.keys()[0]
        key_location = cfg.get_key(aws_key).key_location
        log.info('key_location: %s ' % key_location)
        remote_key_loc = '/root/.ssh/' + key_location.split('/')[-1]
        log.info('remote_key_loc: %s ' % remote_key_loc)
        master.ssh.put(key_location, remote_key_loc)
        master.ssh.chmod(0600, remote_key_loc)
        log.info('Running loadbalancer on the master')
        self._run_load_balancer(master)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        log.info("Loadbalancer only runs on master")

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        log.info("No action required on node removal")


    def _install_starcluster(self, node, starcluster, branch=None):
        dirpath = tempfile.mkdtemp()
        clone = 'git clone %s ' % starcluster
        if branch is not None:
            clone += ' -b %s ' % branch
        clone += dirpath
        node.ssh.execute(clone)
        install = 'cd %s && python setup.py install' % dirpath
        node.ssh.execute(install)
        node.ssh.execute('rm -rf %s' % dirpath)

    def _run_load_balancer(self, node, screen="loadbalancer"):
        sg_prefix = static.SECURITY_GROUP_PREFIX
        cluster_name = node.parent_cluster.name.split(sg_prefix)[1]
        log.info('Cluster name is %s' % cluster_name)
        if not self._check_screen_session(node):
            node.ssh.execute('screen -dmS %s' % screen)
        loadbalancer_cmd = ('/usr/local/bin/starcluster loadbalance '
                            ' --min_nodes %s --max_nodes %s ')
        if self.kill_cluster:
            loadbalancer_cmd += " --kill-cluster "
        if self.options is not None:
            loadbalancer_cmd += self.options + " "

        loadbalancer_cmd += cluster_name
        loadbalancer_cmd = loadbalancer_cmd % (self.min_nodes,
                                               self.max_nodes)
        log.info(loadbalancer_cmd)
        cmd = 'screen -S %s -p 0 -X stuff "%s\n"' % (screen,
                                                     loadbalancer_cmd)
        if not self._check_load_balancer(node):
            node.ssh.execute(cmd)

    def _check_screen_session(self, node, screen="loadbalancer"):
        screen_info = node.ssh.execute('screen -ls | grep %s' %
                                       screen, raise_on_failure=False,
                                       ignore_exit_status=True)
        if screen_info:
            return True
        else:
            return False

    def _check_load_balancer(self, node):
        balancer = node.ssh.execute('ps ax | grep "[s]tarcluster loadbalance"',
                                    raise_on_failure=False,
                                    ignore_exit_status=True)
        if balancer:
            return True
        else:
            return False

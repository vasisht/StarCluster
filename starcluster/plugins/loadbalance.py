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
import tempfile


class LoadBalance(clustersetup.DefaultClusterSetup):
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

    """

    def __init__(self, min_nodes=2, max_nodes=10, kill_cluster=True,
                 starcluster=None, branch=None, options=None,
                 config_file=None):
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes
        self.kill_cluster = kill_cluster
        self.starcluster = starcluster
        self.branch = branch
        self.options = options
        self.config_file = config_file

    def run(self, nodes, master, user, user_shell, volumes):
        if self.starcluster is None:
            log.warn("Missing starcluster parameter")
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
        log.info('Running loadbalancer on the master')
        self._run_load_balancer(master)

    def _install_starcluster(self, node, starcluster, branch=None):
        node.apt_install('libffi-dev')
        dirpath = tempfile.mkdtemp()
        clone = 'git clone %s ' % starcluster
        if branch is not None:
            clone += ' -b %s ' % branch
        clone += dirpath
        node.ssh.execute(clone)
        install = 'cd StarCluster && python setup.py install'
        node.ssh.execute(install)
        node.ssh.execute('rm -rf %s' % dirpath)

    def _run_load_balancer(self, node, screen="loadbalancer"):
        sg_prefix = static.SECURITY_GROUP_PREFIX
        cluster_name = node.parent_cluster.name.split(sg_prefix)[1]
        log.info('Cluster name is %s' % cluster_name)
        if not self._check_screen_session(node):
            node.ssh.execute('screen -dmS %s' % screen)
        loadbalancer_cmd = ('/usr/local/bin/starcluster loadbalance '
                            ' --max_nodes %s --min_nodes %s ')
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

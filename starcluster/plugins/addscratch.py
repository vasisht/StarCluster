# Copyright 2020 Vasisht Tadigotla
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
import json


class AddScratch(clustersetup.DefaultClusterSetup):
    """Add scratch space to nodes

    Example config:

    [plugin addscratch]
    SETUP_CLASS = starcluster.plugins.addscratch.AddScratch
    location=/mnt

    """

    def __init__(self, location='/mnt', **kwargs):
        self.location = location
        super(AddScratch, self).__init__(**kwargs)

    def run(self, nodes, master, user, user_shell, volumes):
        self._master = master
        location = self.location
        for node in nodes:
            log.info('Configuring scratch on %s' % (node.alias))
            self._configure_scratch(node)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        location = self.location
        log.info('Configuring scratch on %s' % (node.alias))
        self._configure_scratch(node)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        log.info('No action required on node removal')

    def _configure_scratch(self, node):
        mount_info = node.ssh.execute('grep %s /proc/mounts' %
                                      self.location, raise_on_failure=False,
                                      ignore_exit_status=True)
        if mount_info:
            log.warn('%s is already a mount point' % self.location)
            log.info(mount_info[0])
        else:
            # Get a list of block devices
            blk_info = node.ssh.execute('lsblk -J', raise_on_failure=False,
                                        ignore_exit_status=True)
            blk_info = ''.join(blk_info)
            blks = json.loads(blk_info)
            mount_devs = []
            for devs in blks['blockdevices']:
                # hacky way to get the nitro devices that are not mounted
                if 'children' not in devs:
                    mount_devs.append(devs['name'])
            log.info('Creating Ext4 file system on %s' % (node.alias))
            node.ssh.execute('mkfs.ext4 /dev/%s' % (mount_devs[0]))
            log.info('Mounting scratch space')
            node.ssh.execute('mount /dev/%s %s' % (mount_devs[0],
                                                   self.location))
            log.info('Setting up scratch permissions')
            node.ssh.execute('mkdir -p /mnt/sgeadmin')
            node.ssh.execute('chown -R sgeadmin.sgeadmin /mnt/sgeadmin')

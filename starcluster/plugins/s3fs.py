# Copyright 2017 Vasisht Tadigotla
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


class S3fs(clustersetup.DefaultClusterSetup):
    """Mount S3 as a file system

    Example config:

    [plugin s3fs]
    SETUP_CLASS = starcluster.plugins.s3fs.S3fs
    bucket_name = mybucket
    mount_point = /data

    """

    def __init__(self, bucket_name=None, mount_point=None, **kwargs):
        self.bucket_name = bucket_name
        self.mount_point = mount_point
        super(S3fs, self).__init__(**kwargs)

    def run(self, nodes, master, user, user_shell, volumes):
        self._master = master
        if self.bucket_name is None:
            log.fatal('Missing required bucket_name')
        if self.mount_point is None:
            log.warn('Missing required mount_point')
        for node in nodes:
            log.info('Mounting %s on %s' % (self.bucket_name, node.alias))
            self._mount_s3fs_on_node(node)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self._master = master
        log.info('Mounting %s on %s' % (self.bucket_name, node.alias))
        self._mount_s3fs_on_node(node)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        self._master = master
        log.info('No action required on node removal')

    def _get_aws_creds(self):
        creds = self._master.ec2.__dict__
        return (creds.get('aws_access_key_id'),
                creds.get('aws_secret_access_key'))

    def _mount_s3fs_on_node(self, node):
        s3_creds = self._get_aws_creds()
        s3fs_string = '%s:%s' % (s3_creds[0], s3_creds[1])
        if not node.ssh.path_exists(self.mount_point):
            node.ssh.makedirs(self.mount_point, mode=0777)
        mount_info = node.ssh.execute('grep %s /proc/mounts' %
                                      self.mount_point, raise_on_failure=False,
                                      ignore_exit_status=True)
        node.ssh.execute('echo %s > /root/.passwd-s3fs' % s3fs_string)
        node.ssh.execute('chmod 600 /root/.passwd-s3fs')
        if mount_info:
            log.warn('%s is already a mount point' % self.mount_point)
        else:
            log.info('Mounting %s on %s' % (self.bucket_name, node.alias))
            node.ssh.execute('s3fs %s %s -o umask=0022 -o allow_other' %
                             (self.bucket_name, self.mount_point))

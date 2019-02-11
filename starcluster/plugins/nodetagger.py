# Copyright 2019 Vasisht Tadigotla
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


class NodeTagger(clustersetup.DefaultClusterSetup):
    """Add AWS tags to nodes

    Example config:

    [plugin nodetagger]
    SETUP_CLASS = starcluster.plugins.nodetagger.NodeTagger
    tags = tag1=value1,tag2=value2

    """

    def __init__(self, tags=None, **kwargs):
        self.tags = dict([tag.split('=') for tag in tags.split(',')])
        super(NodeTagger, self).__init__(**kwargs)

    def run(self, nodes, master, user, user_shell, volumes):
        self._master = master
        tags = self.tags
        for tag in tags:
            log.info('Applying tag %s:%s to master' % (tag, tags[tag]))
            master.add_tag(tag, tags[tag])
            for node in nodes:
                log.info('Applying tag %s:%s to %s' % (tag, tags[tag],
                                                       node.alias))
                node.add_tag(tag, tags[tag])

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        for tag in tags:
            log.info('Applying tag %s:%s to %s' % (tag, tags[tag], node.alias))
            node.add_tag(tag, tags[tag])

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        log.info('No action required on node removal')

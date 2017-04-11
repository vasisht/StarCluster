# Copyright 2017 Vasisht Tadigotla
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
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
import re


class RPackageInstaller(clustersetup.DefaultClusterSetup):
    """
    This plugin installs R packages on all nodes in the cluster. The
    packages are specified in the plugin's config:

    [plugin Rpkginstaller]
    setup_class = starcluster.plugins.Rpkginstaller.RPackageInstaller
    packages = ggplot2, DESeq2
    update = False
    """

    def __init__(self, packages=None, update=None):
        super(RPackageInstaller, self).__init__()
        self.packages = packages
        self.update = str(update).lower() == "true"
        if packages:
            self.packages = [pkg.strip() for pkg in packages.split(',')]

    def run(self, nodes, master, user, user_shell, volumes):
        if not self.packages:
            log.info("No packages specified!")
            return
        log.info('Installing the following packages on all nodes:')
        log.info(', '.join(self.packages), extra=dict(__raw__=True))
        for node in nodes:
            pkgs = ', '.join(self.packages)
            log.info('Installing %s on %s:' % (pkgs, node.alias))
            self._install_R_package(node)

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        pkgs = ', '.join(self.packages)
        log.info('Installing %s on %s:' % (pkgs, node.alias))
        self._install_R_package(node)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        raise NotImplementedError("on_remove_node method not implemented")

    def _install_R_package(self, node):
        repo = "\"http://cran.r-project.org\""
        for pkg in self.packages:
            # Try to install from CRAN
            cmd = "R -e 'install.packages(\"%s\",repos=%s)'" % (pkg, repo)
            inst_info = node.ssh.execute(cmd, raise_on_failure=False,
                                         ignore_exit_status=True)
            if re.search('not available', ','.join(inst_info)):
                # If not in CRAN, use BioConductor
                log.info('Installing %s from BioConductor' % pkg)
                bio = "source(\"https://bioconductor.org/biocLite.R\")"
                if self.update:
                    cmd = "R -e '%s; biocLite(\"%s\",ask=F)'" % (bio, pkg)
                    # Set a very high timeout to install updated dependencies
                    node.ssh._timeout = 1200
                    node.ssh.execute(cmd, raise_on_failure=False,
                                     ignore_exit_status=True)
                else:
                    cmd = ("R -e '%s; biocLite(\"%s\","
                           "suppressUpdates=T)'") % (bio, pkg)
                    # Increase default ssh timeout for installation
                    node.ssh._timeout = 180
                    node.ssh.execute(cmd, raise_on_failure=False,
                                     ignore_exit_status=True)

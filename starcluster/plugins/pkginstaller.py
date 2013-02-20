from starcluster import clustersetup
from starcluster.logger import log


class PackageInstaller(clustersetup.DefaultClusterSetup):
    """
    This plugin installs Ubuntu packages on all nodes in the cluster. The
    packages are specified in the plugin's config:

    [plugin pkginstaller]
    setup_class = starcluster.plugins.pkginstaller.PackageInstaller
    packages = mongodb, python-mongodb
    """
    def __init__(self, packages=None, update=False):
        super(PackageInstaller, self).__init__()
        self.packages = packages
        self.update = update
        if packages:
            self.packages = [pkg.strip() for pkg in packages.split(',')]

    def run(self, nodes, master, user, user_shell, volumes):
        if not self.packages:
            log.info("No packages specified!")
            return
        if self.update:
            log.info('Updating packages on all nodes:')
            for node in nodes:
                self.pool.simple_job(node.apt_command('update'), [], jobid=node.alias)
            self.pool.wait(len(nodes))

        log.info('Installing the following packages on all nodes:')
        log.info(', '.join(self.packages), extra=dict(__raw__=True))
        pkgs = ' '.join(self.packages)
        for node in nodes:
            self.pool.simple_job(node.apt_install, (pkgs), jobid=node.alias)
        self.pool.wait(len(nodes))

    def on_add_node(self, new_node, nodes, master, user, user_shell, volumes):
        log.info('Installing the following packages on %s:' % new_node.alias)
        pkgs = ' '.join(self.packages)
        new_node.apt_install(pkgs)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        raise NotImplementedError("on_remove_node method not implemented")

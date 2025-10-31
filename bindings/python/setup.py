from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
import subprocess
import sys
import os


class PostInstallCommand(install):
    """Post-installation: download native library."""
    def run(self):
        install.run(self)
        # Run download script after install
        script_path = os.path.join(os.path.dirname(__file__), 'download_libs.py')
        subprocess.check_call([sys.executable, script_path])


class PostDevelopCommand(develop):
    """Post-develop: download native library for editable installs."""
    def run(self):
        develop.run(self)
        # Run download script after develop install
        script_path = os.path.join(os.path.dirname(__file__), 'download_libs.py')
        subprocess.check_call([sys.executable, script_path])


# Determine which native library to include based on platform
package_data = {}
if sys.platform == 'darwin':
    package_data['jasonisnthappy'] = ['lib/**/*.dylib']
elif sys.platform.startswith('linux'):
    package_data['jasonisnthappy'] = ['lib/**/*.so']
elif sys.platform == 'win32':
    package_data['jasonisnthappy'] = ['lib/**/*.dll']

setup(
    name="jasonisnthappy",
    packages=find_packages(),
    package_data=package_data,
    include_package_data=True,
    cmdclass={
        'install': PostInstallCommand,
        'develop': PostDevelopCommand,
    },
)

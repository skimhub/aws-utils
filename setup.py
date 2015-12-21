from os import path
from setuptools import setup, find_packages


package = 'aws-utils'
version = '0.1'

# REQ_FILE = path.join(path.dirname(__file__), 'requirements.txt')
# REQUIREMENTS = [x.strip() for x in open(REQ_FILE)]

setup(
    name=package,
    version=version,
    description="collection of AWS useful functions",
    url='https://github.com/skimhub/aws-utils',
    packages=find_packages(exclude=['tests']),
    package_data={'': ['requirements.txt']},
    # install_requires=REQUIREMENTS,
)

from setuptools import setup, find_packages

package = 'aws-utils'
version = '0.1'


INSTALL_REQUIRES = [
    'boto==2.38.0',
    'boto3==1.2.3',
    'botocore==1.3.14',
    'bz2file==0.98',
    'docutils==0.12',
    'futures==3.0.3',
    'httpretty==0.8.10',
    'jmespath==0.9.0',
    'python-dateutil==2.4.2',
    'requests==2.8.1',
    'six==1.10.0',
    'smart-open==1.3.1',
    'wheel==0.24.0',
]

TEST_REQUIRES = [
    'moto==0.4.19',
    'pytest==2.8.5',
]

setup(
    name=package,
    version=version,
    description="collection of AWS useful functions",
    url='https://github.com/skimhub/aws-utils',
    packages=find_packages(),
    package_data={'': ['requirements.txt']},
    install_requires=INSTALL_REQUIRES,
    test_requires=TEST_REQUIRES,
)

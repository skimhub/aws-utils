from setuptools import setup, find_packages

package = 'aws-utils'
version = '0.2.2'

INSTALL_REQUIRES = [
    'boto>=2.38.0,<3.0.0',
    'boto3>=1.2.3,<1.3.0',
    'smart-open>=1.3.1,<1.4.0',
    'dateutils>=0.6.6',
]

TEST_REQUIRES = [
    'moto>=0.4.19,<0.5.0',
    'pytest>=2.8.5,<2.9.0',
    'mock==1.3.0'
]

setup(
    name=package,
    version=version,
    author="Skimlinks Ltd.",
    author_email="dev@skimlinks.com",
    description="collection of AWS useful functions",
    url='https://github.com/skimhub/aws-utils',
    packages=find_packages(exclude=['test']),
    install_requires=INSTALL_REQUIRES + TEST_REQUIRES, # not entirely corect but gets tests with moto working
    # test_require=TEST_REQUIRES,
)

from setuptools import setup, find_packages

package = 'aws-utils'
version = '0.3.0'

INSTALL_REQUIRES = [
    'boto>=2.38.0',
    'boto3>=1.2.3',
    'smart_open==1.3.2',  # smart open must be 1.3.2 because in 1.3.3 onward the gzip write functionality has been removed
    'dateutils>=0.6.6',
    'retrying>=1.3.3'
]

setup(
    name=package,
    version=version,
    author="Skimlinks Ltd.",
    author_email="dev@skimlinks.com",
    description="collection of AWS useful functions",
    url='https://github.com/skimhub/aws-utils',
    packages=find_packages(exclude=['test']),
    install_requires=INSTALL_REQUIRES,
)

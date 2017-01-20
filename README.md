# Aws utils

Collection of code snippets used for AWS (S3/EMR and so on).

## Testing

We should aim > 90% test coverage, using the Moto library to properly
test all the boto function calls.

This project should be made working both with Python2.7 and Python>=3.4
so making sure all the libraries declared here are Python3 Friendly.


## Running tests

This test uses *tox* to run tests for multiple versions of Python, so
all you need to do to run your tests is to install tox on your system
and run it:

```shell

   $ tox
   py27: commands succeeded
   py34: commands succeeded
   py35: commands succeeded
   congratulations :)
```

## Deploying

**devpi**  
*Dev libraries : http://pypi.skimlinks.net/skim/dev*  
*Prod libraries : http://pypi.skimlinks.net/skim/live*  

```
1) Within the project's root :
pip install devpi
devpi login skim --password <devPI_pass>

2) Edit setup.py :
version=<some_package_number>

3) Upload to http://pypi.skimlinks.net/skim/live :
devpi upload

4.1) Install with :
pip install -r services/requirements.txt --index-url=http://pypi.skimlinks.net/skim/live --trusted-host pypi.skimlinks.net
<OR>
4.2) Add to requirements :
or add to requirements as : audience-recommendation==<some_package_number>
```
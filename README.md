# Aws utils

Collection of code snippets used for AWS (S3/EMR and so on).

## Testing

We should aim > 90% test coverage, having at least one full
integration test for each function.

To make it more clear tests should be split in the *unit* and
*integration* directory.

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

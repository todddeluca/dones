
## Introduction

The dones module can be used to mark whether a key is "done" and check whether
a key has been marked "done".  Keys can also be unmarked, so that they are no
longer "done".  Also, all keys can be unmarked by clearing the Dones.
Keys are kept in their own namespace to avoid conflicts with other
sets of other keys and to make it easy to implement clearing.

Why?  I use `dones` to keep track of what I've already done.  More
specifically, I run large computations (a few million tasks) on a large cluster
(a few thousand cores) with a slow filesystem (Isilon).  When tasks inevitably
fail, perhaps because the network storage goes offline, or a computer dies, or
another user overwhelms the database with connections, I need to resubmit the
tasks to the batch queuing system (LSF) of the cluster that are not already
done.  

The solution in this module fits my constraints.  It handles the concurrent
writes of a thousand jobs marking things done (not all at once).  It is
reasonably fast for reading and writing up to millions of jobs.  This is
important because or batch queue (LSF) only handles a few thousand jobs at a
time gracefully.  Finally, `dones` uses MySQL as a backend, which is important
because I cannot run a key-value server like Redis on the cluster I use.


## Contribute

Feel free to make a pull request on github.


## Testing

Awkwardly, `dones` is configured with a MySQL database url from the
environment, so to test it, you need to add a url.  For example:

    DONES_DB_URL=mysql://myuser:password@localhost/mydb nosetests


## Requirements

- Probably Python 2.7 (since that is the only version it has been tested with.)
- MySQL-python PyPI package.


## Installation


### Install from pypi.python.org

Download and install using pip:

    pip install dones


### Install from github.com

Using github, one can clone and install a specific version of the package:

    cd ~
    git clone git@github.com:todddeluca/dones.git
    cd dones
    python setup.py install

Or use pip:

    pip install git+git://github.com/todddeluca/dones.git#egg=dones


## Usage


    import dones

    if not dones.get('my_pipeline').done('task1'):
        dotask('task1')
        dones.get('my_pipleline').mark('task1')






import os

import dones


TEST_NS = 'test_f2b12c28eb504921bce4bd57da955c25' # a UUID


def test_dburl():
    '''
    No point in testing if there is no database url for the dones to use.
    '''

    assert os.environ.get('DONES_DB_URL')


def test_module_dones():

    assert not dones.get(TEST_NS).done('foo')
    dones.get(TEST_NS).mark('foo')
    assert dones.get(TEST_NS).done('foo')
    dones.get(TEST_NS).mark('foo')
    assert dones.get(TEST_NS).done('foo')
    dones.get(TEST_NS).unmark('foo')
    assert not dones.get(TEST_NS).done('foo')
    dones.get(TEST_NS).unmark('foo')
    assert not dones.get(TEST_NS).done('foo')
    dones.get(TEST_NS).mark('foo')
    assert dones.get(TEST_NS).done('foo')

    # clean up by removing the table.
    dones.get(TEST_NS).clear()


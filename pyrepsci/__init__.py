#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

# Parameter definitions
# ============================================================================

complib = 'blosc'
complevel = 9

path_data = 'data'
path_src = 'source_data'
path_doc = 'documentation'


# Library imports
# ============================================================================

import datetime
import inspect
import os
import pkg_resources
import tempfile
try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve

# TODO: Check if this requirement can be lowered. It doesn't work with
#       0.8.1
pkg_resources.require("pandas>=0.11.0")
import pandas as pd
from tables.nodes import filenode


# Save script to datastore
# ============================================================================

def save_script_to_datastore(datastore, filename="prepare_data.py",
        path=path_doc):
    """Save the current Python script to a datastore."""
    caller = inspect.getframeinfo(inspect.currentframe().f_back)[0]
    with open(caller, "r") as fd:
        DATA = fd.read()
    store = pd.HDFStore(datastore, "a")
    if path not in [n.__repr__().split("(")[0].strip()[1:] for n in
            store._handle.iterNodes('/')]:
        store._handle.createGroup('/', path)
    if path[0] != "/":
        path = "/{0}".format(path)
    store._handle.createArray(path, filename, DATA)
    store.close()


# Re-create source data file to local file
# ============================================================================

def retrieve_srcdata(datastore, data_filename, output_filename):
    store = pd.HDFStore(datastore, "r")
    fnode = filenode.openNode(store._handle.getNode(where="/{0}".format(path_src),
            name=data_filename))
    DATA = fnode.read()
    fnode.close()
    with open(output_filename, "wb") as fd:
        fd.write(DATA)
    del DATA
    store.close()


# Save pandas object
# ============================================================================

def save_pandas(datastore, dataobj, varname, datasource_filename=None):
    if isinstance(datastore, pd.io.pytables.HDFStore):
        datastore["/{0}/{1}".format(path_data, varname)] = dataobj
        if datasource_filename:
            datastore.get_storer("/{0}/{1}".format(path_data, varname)).attrs.DATASOURCE_FILENAME = datasource_filename
    elif isinstance(datastore, str):
        store = pd.HDFStore(datastore, "a")
        store["/{0}/{1}".format(path_data, varname)] = dataobj
        if datasource_filename:
            store.get_storer("/{0}/{1}".format(path_data, varname)).attrs.DATASOURCE_FILENAME = datasource_filename
        store.close()
    else:
        raise NotImplementedError()


# Retrieve data file from internet and store it in datastore
# ============================================================================

def download_and_store(url, datastore, data_filename=None, overwrite=False,
        download_reference=None):
    # set data_filename from the URL if not explicitly defined
    if not data_filename:
        data_filename = url.split('/')[-1]
    # if datastore exists, check if filename already exists in datastore
    if os.access(datastore, os.R_OK):
        store = pd.HDFStore(datastore, "a")
        src_filenames = [n.__repr__().split("(")[0].strip()[1:].split("/")[1]
                for n in store._handle.iterNodes('/{0}'.format(path_src))]
        if data_filename in src_filenames:
            if overwrite:
                store._handle.removeNode("/{0}/{1}".format(path_src,
                        data_filename))
            else:
                raise AttributeError("Data file cannot be saved: filename "
                        "already exists in datastore")
    # create datastore
    else:
        store = pd.HDFStore(datastore, "w", complevel=complevel,
                complib=complib)
    # prepare datastore
    if path_data not in [n.__repr__().split("(")[0].strip()[1:] for n in
            store._handle.iterNodes('/')]:
        store._handle.createGroup('/', path_data)
    if path_src not in [n.__repr__().split("(")[0].strip()[1:] for n in
            store._handle.iterNodes('/')]:
        store._handle.createGroup('/', path_src)
    if path_doc not in [n.__repr__().split("(")[0].strip()[1:] for n in
            store._handle.iterNodes('/')]:
        store._handle.createGroup('/', path_doc)
    # retrieve file
    # TODO: find out if download was successful
    localfile = tempfile.mktemp()
    urlretrieve(url, localfile)
    with open(localfile, "rb") as fd:
        rawdata = fd.read()
    # save retrieved file to datastore
    fnode = filenode.newNode(store._handle, where="/{0}".format(path_src),
            name=data_filename)
    fnode.write(rawdata)
    fnode.close()
    # create metadata
    srcnode = store._handle.getNode("/{0}".format(path_src), data_filename)
    srcnode.attrs.DOWNLOAD_URL = url
    srcnode.attrs.DOWNLOAD_DATE = datetime.datetime.now().isoformat()
    if download_reference:
        srcnode.attrs.DOWNLOAD_REFERENCE = download_reference
    # TODO: create README
    # cleanup
    os.remove(localfile)
    store.close()


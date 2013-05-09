#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

# Indian states' SDP at current prices
# ============================================================================
#
# :Author: Andreas.Hilboll@uni-bremen.de
# :Date:   2013-05-09
#
# This script downloads and stores Indian states' SDP at current prices from
# the website of the Indian Ministry of Statistics and Programme
# Implementation.

# Parameter definitions
# ============================================================================

url = "http://mospi.nic.in/Mospi_New/upload/Item_12_SDP-2004-05_2april13.xls"
original_filename = url.split("/")[-1]
output_file = "india_sdp.h5"
download_reference = "http://mospi.nic.in/Mospi_New/site/inner.aspx?status=3&menu_id=82"


# Library imports
# ============================================================================

import os
import tempfile
import pandas as pd
import pyrepsci as rs


# Download original data
# ============================================================================

rs.download_and_store(url, output_file, download_reference=download_reference,
        overwrite=True)


# Read and prepare original data
# ============================================================================

# read original Excel file
xls_file = tempfile.mktemp(suffix=".xls")
rs.retrieve_srcdata(output_file, original_filename, xls_file)
# Create XLS parser object
xls_obj = pd.ExcelFile(xls_file)
# Read the XLS sheet with Indian states' SDP at current prices
DATA = xls_obj.parse(sheetname="SDP-Curr", header=4, skiprows=[5, 6],
        index_col=2, skip_footer=5)
# Remove columns with junk
DATA = DATA.drop(["Sl.", "State\UT"], axis=1)
# Remove columns with relative changes, because we don't need this
dropcols = [c for c in DATA.columns if c[-2:] == ".1"]
DATA = DATA.drop(dropcols, axis=1).T
# Re-index so that we have a pd.PeriodIndex
DATA.index = pd.period_range(start="2005", end="2013", freq="A-MAR")
# Rename "Andaman & Nicobar Islands" to "Andaman & Nicobar"
DATA.columns = [str(s) if s != "Andaman & Nicobar Islands" else
        "Andaman & Nicobar" for s in DATA.columns]
# TODO: Store data in datastore
rs.save_pandas(output_file, DATA, "indian_states_sdp_at_current_prices",
        datasource_filename=original_filename)
# Cleanup.
del xls_obj
os.remove(xls_file)


# Save this script
# ============================================================================

rs.save_script_to_datastore(output_file)


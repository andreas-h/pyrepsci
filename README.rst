pyrepsci
========

*pyrepsci* is a simple toolbox for reproducible science.

Currently, it supports storing data files inside a ``pandas.HDFStore``. The
idea is to be able to have the original data file, the script which converts
the original data file to a pandas object, and the pandas object all within
one file. The goal is tracability of the origin of the data which is used in
research.


Using the extractor:

The first argument must always be a valid EDP username (eXXXXXX). The password is requested during execution.

Tags are added through a .csv file, in which case you must add -csv <path to file> as arguments. An example file is supplied.
The format is SERVERNAME;TAG[;STARTDATE]. Start date is optional, and allows you to extract values starting from that specified date.
If omitted, a default timestamp is used, which covers all values from the very beginning.

By default, raw values are extracted. Add -interpolated to get interpolated values instead.
Note: Functions for interpolated data were made very last-minute, so the sync time and interval parameters are just defaults.
That should be a quick fix though.


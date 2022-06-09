pyqc
--------
``v0.1.7``

Added to 0.1.7:
 - Added QC167
 - Moved emailing script to Google API
 
Added to 0.1.6:
Error emailing script fixed

Added to 0.1.5:
SP to upload kit QC data

Package contains all the relevant scripts to pull manufacturing and QC data from all the Reagent Manufacturing process areas. 
The flagship functions of this package are the ``run_pipeline`` scripts that will read the files from Box or relevant locations, transform them into dataframes,
and push the dataframes to the CPPDA postgres database.

The scripts are structured in the following way
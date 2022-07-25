pyqc
--------
``v0.1.9``

Added to 0.1.9:
 - Added QC149 (rev F)

Added to 0.1.8:
 - Added Rev P to QC123
 - Added Rev C to QC167

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
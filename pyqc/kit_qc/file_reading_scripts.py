import pandas as pd
import datetime as dt
import numpy as np
from pyqc.send_error_emails import send_error_emails


def read_funcseq_qc_data(file):

    # creating a list of errors, later to be mailed to the qc folks
    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary- QC Records", header=None)

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]

        # check if qc_by is empty
        if qc_by == "Enter Here":
            print("File not ready for ingestion")
            return pd.DataFrame()

        try:
            # just a check if the name has been entered in the first.last format
            qc_tech_name = qc_by.split(".")[1].capitalize()
        except:
            error_log.append("'QC by name' entered wrongly")
        
        # extract the date field as it is
        date_string = df_temp[df_temp[1].str.contains("QC end date", na=False)].iloc[
            0, 2
        ]

        # if the field is a string, convert it into a datetime object and then into
        # a properly formatted (Ymd) string
        if isinstance(date_string, str) and date_string != "Enter Here":
            e_date = dt.date.strftime(
                dt.datetime.strptime(date_string, "%d-%m-%Y"), "%Y-%m-%d"
            )
            # making this assumption since CA enters date in a particular format
            file_loc = "CA"
        # if field is empty, somehow its registered as a float or sometime its
        # just "Enter Here"
        # this is used as a check if the QC document is complete for ingestion
        # if incomplete, skip reading this file
        elif isinstance(date_string, float) or date_string == "Enter Here":
            print("Sequencing results not yet available")
            return pd.DataFrame()
        # if some other format, try converting to proper format string
        # if this fails, the try catch will catch this and add it to the
        # error log
        else:
            e_date = dt.date.strftime(date_string, "%Y-%m-%d")
            # another assumption based on SG date format
            file_loc = "SG"

        df_temp1 = pd.read_excel(xlsx, sheet_name="Disposition", header=None)

        ## Extract Test Results
        # get indices of rows that have the below string
        data_s = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each TEST Sample", na=False)
        ].tolist()
        data_e = df_temp1.index[
            df_temp1[0].str.contains("Control Results", na=False)
        ].tolist()
        # get all the rows lying in the block
        data1 = df_temp1[data_s[0] : data_s[1]]
        # select only relevant columns
        data1 = data1[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        # set first rows as column names
        data1.columns = data1.iloc[0]
        # delete first row, since these are just column names
        data1 = data1.iloc[1:, :]
        # reset index
        data1.reset_index(drop=True, inplace=True)
        # deleting all rows that have NAs in the 'Sequencer' column.
        # this ensures that we only get rows this filled metrics
        data1 = data1.dropna(subset=["Sequencer"])

        # same as above for second block of metrics
        data2 = df_temp1[data_s[1] : data_e[0]]
        data2 = data2[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Sequencer"])

        # join first block and second block of results
        result = pd.concat([data1, data2], axis=1, join="inner")
        result = result.loc[:, ~result.columns.duplicated()]
        # add new column which differentiates test and control results
        result = result.assign(family="test")

        ## Extract Control Results
        # exactly the same procedure as above
        data_s = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each CONTROL Sample", na=False)
        ].tolist()
        data1 = df_temp1[data_s[0] : data_s[1]]
        data1 = data1[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data1.columns = data1.iloc[0]
        data1 = data1.iloc[1:, :]
        data1.reset_index(drop=True, inplace=True)
        data1 = data1.dropna(subset=["Sequencer"])

        data2 = df_temp1[data_s[1] : data_s[1] + 9]
        data2 = data2[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Sequencer"])

        control = pd.concat([data1, data2], axis=1, join="inner")
        control = control.loc[:, ~control.columns.duplicated()]
        control = control.assign(family="control")

        # Now add the control and test blocks
        data = result.append(control)
        # pivot the above dataframe to a long format
        data = data.melt(
            id_vars=["family", "Sequencer", "Description"], var_name="metric"
        )
        # add meta data to the dataframe
        data = data.assign(filename=file, date=e_date, qc_by=qc_by)

        if len(data)  == 0:
            error_log.append("No Disposition Data Entered")

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking", header=None)
        data_s = df_ln.index[
            df_ln[1].str.contains("Single Cell Library reagents", na=False)
        ].tolist()
        data_e = df_ln.index[
            df_ln[1].str.contains("Single Index Kit T", na=False)
        ].tolist()
        lns = df_ln[data_s[0] : data_e[0]]
        # select relevant columns
        lns = lns[[0, 1, 2, 3, 4]]
        # set column names from first row
        lns.columns = lns.iloc[0]
        lns = lns.iloc[1:, :]
        lns.reset_index(drop=True, inplace=True)
        # rename columns
        lns.columns = ["subname", "pn_descrip", "pn", "test", "control"]

        # create new description column
        lns[['subname']] = lns[['subname']].fillna('')
        lns['pn_descrip'] = lns['pn_descrip'] + lns['subname']
        lns = lns.drop(columns=['subname'])

        # drop rows that are not associated with any part number and description
        lns = lns.dropna(subset=["pn_descrip", "pn"])
        # if a column has 'Enter Here', replace that by NA
        lns = lns.replace("Enter Here", np.nan)
        # drop all rows where both test and control columns have NAs
        lns = lns.dropna(subset=["test", "control"], how="all")
        # If any of the test or control columns are filled with the other unfilled
        # set the check as 'Fail'. Since we want both of them to be filled
        lns["check"] = np.where(
            (lns["test"].notnull()) & (lns["control"].notnull()),
            "Pass",
            "Fail",
        )

        # if any of the rows 'Fail' the check, raise an Error
        if (lns["check"] == "Fail").any():
            raise NameError("The control and test lots not entered properly")

        lns = lns[lns["check"] == "Pass"]
        lns = lns.drop(columns=["check"])

        # Pivot rows to long format
        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        # add file name (metadata) column
        lns = lns.assign(filename=file)

        # merge the lot information with previous metrics information
        # we are creating a staging table here
        # it will be normalized and stored in the DB using a stored procedure
        df = pd.merge(data, lns, on=["filename", "family"])

        # cleaning column names
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace(".", "")
            .str.replace("<", "lessthan")
            .str.replace(">", "greaterthan")
        )

    # Now we handle errors that might be raised by any of the above code
    # and add it to the error list
    except ValueError as err:
        # if any of the above sheets can't be read
        if "Worksheet" in err.args[0]:
            msg = f"Summary- QC Records, Disposition or LN Tracking sheet not found in {file}"
            error_log.append(msg)
    except TypeError as err:
        # if the date conversion goes wrong
        if "datetime.date" in err.args[0]:
            msg = f"QC date format incorrect in {file}"
            error_log.append(msg)
    except NameError as err:
        # if any of the control or test lots are missing
        if "entered properly" in err.args[0]:
            msg = f"""The control and test lots not entered properly in {file}. 
                    Make sure control and test lots are entered appropriately."""
            error_log.append(msg)
    except:
        # all other unexpected errors
        msg = f"Other unknown error in {file}. Please reach out to Process Dev Data Analytics."
        error_log.append(msg)

    # if any error is generated, email the owner and supervisor and
    # return an empty dataframe
    if len(error_log) > 1:
        send_error_emails(filename=file, 
                          error_list=error_log, 
                          qc_by=qc_by, 
                          file_loc=file_loc)
        # print(error_log)
        df = pd.DataFrame()

    return df

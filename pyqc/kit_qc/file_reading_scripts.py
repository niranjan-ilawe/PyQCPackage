import re
import pandas as pd
import datetime as dt
import numpy as np
from pyqc.send_error_emails import send_error_emails


def read_qc123_data_revN(file):

    # creating a list of errors, later to be mailed to the qc folks
    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary- QC Records", header=None)

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]
        #pn = df_temp[df_temp[1].str.contains("10X Part Number", na=False)].iloc[0, 2]

        # check if qc_by is empty
        if qc_by == "Enter Here":
            print("File not ready for ingestion")
            return pd.DataFrame()

        wo = df_temp[df_temp[1].str.contains("Work Order", na=False)].iloc[0, 2]

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
            try:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%b-%Y"), "%Y-%m-%d"
                )
            except:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%m-%Y"), "%Y-%m-%d"
                )
            # making this assumption since CA enters date in a particular format
            #file_loc = "CA"
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
            #file_loc = "SG"

        # if the WO starts with a '1' then set file_loc as "CA" if it starts with '2' then 
        # file_loc as "SG"
        if re.search("\d",str(wo)).group(0) == '1':
            file_loc = "CA"
        elif re.search("\d",str(wo)).group(0) == '2':
            file_loc = "SG"
        else:
            file_loc = "NA"
            error_log.append("'WO on summary page is wrong' does not start with 1 or 2")

        try:
            run_num = re.findall('[R|r]un.?(\d)', file)[0]
        except:
            run_num = 1
            #error_log.append("'Run Number not found' add '_run #' to file name")

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
        data = data.assign(wo = wo, filename=file, date=e_date, qc_by=qc_by)

        if len(data)  == 0:
            error_log.append("No Disposition Data Entered")

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking", header=None)

        # product tested for easy linking to trend chart
        product = df_ln[df_ln[0].str.contains("Single Cell Product", na=False)].iloc[0,1]

        data_s = df_ln.index[
            df_ln[0].str.contains("Single Cell Library reagents", na=False)
        ].tolist()
        data_e = df_ln.index[
            df_ln[0].str.contains("Single Index Kit T", na=False)
        ].tolist()
        lns = df_ln[data_s[0] : data_e[0]]
        # select relevant columns
        lns = lns[[0, 1, 2, 3]]
        # set column names from first row
        lns.columns = lns.iloc[0]
        lns = lns.iloc[1:, :]
        lns.reset_index(drop=True, inplace=True)
        # rename columns
        lns.columns = ["pn_descrip", "pn", "test", "control"]

        ## Temp code for adding subname
        ## delete after ECO
        subname = ['','','','','','','','',
                   '','','','','','','',
                   '','','','','','','',
                   '','','','','','','','',
                   '','','GEM Kit','','','','',
                   '','','Library Kit','','','','',
                   '','','','','','','',
                   '','','','','','','']

        lns['subname'] = subname

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

        # check if lots only have the 6 digit wo
        if any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['test']]) or any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['control']]):
            error_log.append("Lots entered on LN Tracking page not correct")

        # Pivot rows to long format
        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        # add file name (metadata) column
        lns = lns.assign(filename=file)

        # merge the lot information with previous metrics information
        # we are creating a staging table here
        # it will be normalized and stored in the DB using a stored procedure
        df = pd.merge(data, lns, on=["filename", "family"])

        # adding the product column previously extracted from the LN tracking tab
        df = df.assign(product = product, runnum = run_num)

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

def read_qc123_data_revP(file):

    # creating a list of errors, later to be mailed to the qc folks
    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary- QC Records", header=None)

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]
        #pn = df_temp[df_temp[1].str.contains("10X Part Number", na=False)].iloc[0, 2]

        # check if qc_by is empty
        if qc_by == "Enter Here":
            print("File not ready for ingestion")
            return pd.DataFrame()

        wo = df_temp[df_temp[1].str.contains("Work Order", na=False)].iloc[0, 2]

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
            try:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%b-%Y"), "%Y-%m-%d"
                )
            except:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%m-%Y"), "%Y-%m-%d"
                )
            # making this assumption since CA enters date in a particular format
            #file_loc = "CA"
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
            #file_loc = "SG"

        # if the WO starts with a '1' then set file_loc as "CA" if it starts with '2' then 
        # file_loc as "SG"
        if re.search("\d",str(wo)).group(0) == '1':
            file_loc = "CA"
        elif re.search("\d",str(wo)).group(0) == '2':
            file_loc = "SG"
        else:
            file_loc = "NA"
            error_log.append("'WO on summary page is wrong' does not start with 1 or 2")

        try:
            run_num = re.findall('[R|r]un.?(\d)', file)[0]
        except:
            run_num = 1
            #error_log.append("'Run Number not found' add '_run #' to file name")

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
        data = data.assign(wo = wo, filename=file, date=e_date, qc_by=qc_by)

        if len(data)  == 0:
            error_log.append("No Disposition Data Entered")

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking", header=None)

        # product tested for easy linking to trend chart
        product = df_ln[df_ln[1].str.contains("Single Cell Product", na=False)].iloc[0,2]

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

        ## Temp code for adding subname
        ## delete after ECO
        # subname = ['','','','','','','','',
        #            '','','','','','','',
        #            '','','','','','','',
        #            '','','','','','','','',
        #            '','','GEM Kit','','','','',
        #            '','','Library Kit','','','','',
        #            '','','','','','','',
        #            '','','','','','','']

        # lns['subname'] = subname

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

        # check if lots only have the 6 digit wo
        if any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['test']]) or any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['control']]):
            error_log.append("Lots entered on LN Tracking page not correct")

        # Pivot rows to long format
        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        # add file name (metadata) column
        lns = lns.assign(filename=file)

        # merge the lot information with previous metrics information
        # we are creating a staging table here
        # it will be normalized and stored in the DB using a stored procedure
        df = pd.merge(data, lns, on=["filename", "family"])

        # adding the product column previously extracted from the LN tracking tab
        df = df.assign(product = product, runnum = run_num)

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

def read_qc167_data_revB(file):

    # creating a list of errors, later to be mailed to the qc folks
    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary-QC records", header=None)

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]

        # check if qc_by is empty
        if qc_by == "Enter Here":
            print("File not ready for ingestion")
            return pd.DataFrame()

        #pn = df_temp[df_temp[1].str.contains("10X Part Number", na=False)].iloc[0, 2]
        wo = df_temp[df_temp[7].str.contains("Yes", na=False)].iloc[0, 8]

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
            try:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%b-%Y"), "%Y-%m-%d"
                )
            except:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%m-%Y"), "%Y-%m-%d"
                )
            # making this assumption since CA enters date in a particular format
            #file_loc = "CA"
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
            #file_loc = "SG"

        # if the WO starts with a '1' then set file_loc as "CA" if it starts with '2' then 
        # file_loc as "SG"
        if re.search("\d",str(wo)).group(0) == '1':
            file_loc = "CA"
        elif re.search("\d",str(wo)).group(0) == '2':
            file_loc = "SG"
        else:
            file_loc = "NA"
            error_log.append("'WO on summary page is wrong' does not start with 1 or 2")

        try:
            run_num = re.findall('[R|r]un.?(\d)', file)[0]
        except:
            run_num = 1
            #error_log.append("'Run Number not found' add '_run #' to file name")

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
        data1 = data1[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        # set first rows as column names
        data1.columns = data1.iloc[0]
        # delete first row, since these are just column names
        data1 = data1.iloc[1:, :]
        # reset index
        data1.reset_index(drop=True, inplace=True)
        # deleting all rows that have NAs in the 'Sequencer' column.
        # this ensures that we only get rows this filled metrics
        data1 = data1.dropna(subset=["Enter Row # for Each TEST Sample"])
        try:
            data1 = data1[data1["Enter Row # for Each TEST Sample"].str.lower() != "enter test here"]
        except:
            print('')
        data1 = data1.drop(columns=['Enter Row # for Each TEST Sample'])

        # same as above for second block of metrics
        data2 = df_temp1[data_s[1] : data_e[0]]
        data2 = data2[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Enter Row # for Each TEST Sample"])
        # now try to clean up the "enter test here" values if present
        try:
            data2 = data2[data2["Enter Row # for Each TEST Sample"].str.lower() != "enter test here"]
        except:
            print("")

        data2 = data2.drop(columns=['Enter Row # for Each TEST Sample'])

        # join first block and second block of results
        result = pd.concat([data1, data2], axis=1, join="inner")
        result = result.loc[:, ~result.columns.duplicated()]
        # add new column which differentiates test and control results
        result = result.assign(family="test")

        ## Extract Control Results
        # exactly the same procedure as above
        data_s1 = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each Control Sample", na=False)
        ].tolist()
        data_s2 = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each CONTROL Sample", na=False)
        ].tolist()
        data1 = df_temp1[data_s1[0] : data_s2[0]]
        data1 = data1[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data1.columns = data1.iloc[0]
        data1 = data1.iloc[1:, :]
        data1.reset_index(drop=True, inplace=True)
        data1 = data1.dropna(subset=["Enter Row # for Each Control Sample"])
        try:
            data1 = data1[data1["Enter Row # for Each Control Sample"].str.lower() != "enter control here"]
        except:
            print('')
        data1 = data1.drop(columns=['Enter Row # for Each Control Sample'])

        data2 = df_temp1[data_s2[0] : data_s2[0] + 9]
        data2 = data2[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Enter Row # for Each CONTROL Sample"])
        try:
            data2 = data2[data2["Enter Row # for Each CONTROL Sample"].str.lower() != "enter control here"]
        except:
            print('')
        data2 = data2.drop(columns=['Enter Row # for Each CONTROL Sample'])

        control = pd.concat([data1, data2], axis=1, join="inner")
        control = control.loc[:, ~control.columns.duplicated()]
        control = control.assign(family="control")

        # Now add the control and test blocks
        data = result.append(control)
        # pivot the above dataframe to a long format
        data = data.melt(
            id_vars=["family", "Description"], var_name="metric"
        )
        # add meta data to the dataframe
        data = data.assign(wo = wo, filename=file, date=e_date, qc_by=qc_by)

        if len(data)  == 0:
            error_log.append("No Disposition Data Entered")

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking", header=None)
        
        # product tested for easy linking to trend chart
        product = df_ln[df_ln[0].str.contains("Single Cell Product", na=False)].iloc[0,1]

        data_s = df_ln.index[
            df_ln[0].str.contains("Single Cell Library reagents", na=False)
        ].tolist()
        data_e = df_ln.index[
            df_ln[0].str.contains("Index Kit T", na=False)
        ].tolist()
        lns = df_ln[data_s[0] : data_e[0]]
        # select relevant columns
        lns = lns[[0, 1, 2, 3]]
        # set column names from first row
        lns.columns = lns.iloc[0]
        lns = lns.iloc[1:, :]
        lns.reset_index(drop=True, inplace=True)

        # rename columns
        lns.columns = ["pn_descrip", "pn", "test", "control"]

        ## Temp code for adding subname
        ## delete after ECO
        subname = ['','','','','','','',
                   '','','','','','',
                   '','','GEM Kit','','','',
                   '','','','','Library Kit','']

        lns['subname'] = subname

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

        # check if lots only have the 6 digit wo
        if any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['test']]) or any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['control']]):
            error_log.append("Lots entered on LN Tracking page not correct")

        # Pivot rows to long format
        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        # add file name (metadata) column
        lns = lns.assign(filename=file)

        # merge the lot information with previous metrics information
        # we are creating a staging table here
        # it will be normalized and stored in the DB using a stored procedure
        df = pd.merge(data, lns, on=["filename", "family"])

        # adding the product column previously extracted from the LN tracking tab
        df = df.assign(product = product, runnum = run_num)

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

def read_qc167_data_revC(file):

    # creating a list of errors, later to be mailed to the qc folks
    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary-QC records", header=None)

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]

        # check if qc_by is empty
        if qc_by == "Enter Here":
            print("File not ready for ingestion")
            return pd.DataFrame()

        #pn = df_temp[df_temp[1].str.contains("10X Part Number", na=False)].iloc[0, 2]
        wo = df_temp[df_temp[7].str.contains("Yes", na=False)].iloc[0, 8]

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
            try:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%b-%Y"), "%Y-%m-%d"
                )
            except:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%m-%Y"), "%Y-%m-%d"
                )
            # making this assumption since CA enters date in a particular format
            #file_loc = "CA"
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
            #file_loc = "SG"

        # if the WO starts with a '1' then set file_loc as "CA" if it starts with '2' then 
        # file_loc as "SG"
        if re.search("\d",str(wo)).group(0) == '1':
            file_loc = "CA"
        elif re.search("\d",str(wo)).group(0) == '2':
            file_loc = "SG"
        else:
            file_loc = "NA"
            error_log.append("'WO on summary page is wrong' does not start with 1 or 2")

        try:
            run_num = re.findall('[R|r]un.?(\d)', file)[0]
        except:
            run_num = 1
            #error_log.append("'Run Number not found' add '_run #' to file name")

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
        data1 = data1[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        # set first rows as column names
        data1.columns = data1.iloc[0]
        # delete first row, since these are just column names
        data1 = data1.iloc[1:, :]
        # reset index
        data1.reset_index(drop=True, inplace=True)
        # deleting all rows that have NAs in the 'Sequencer' column.
        # this ensures that we only get rows this filled metrics
        data1 = data1.dropna(subset=["Enter Row # for Each TEST Sample"])
        try:
            data1 = data1[data1["Enter Row # for Each TEST Sample"].str.lower() != "enter test here"]
        except:
            print('')
        data1 = data1.drop(columns=['Enter Row # for Each TEST Sample'])

        # same as above for second block of metrics
        data2 = df_temp1[data_s[1] : data_e[0]]
        data2 = data2[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Enter Row # for Each TEST Sample"])
        # now try to clean up the "enter test here" values if present
        try:
            data2 = data2[data2["Enter Row # for Each TEST Sample"].str.lower() != "enter test here"]
        except:
            print("")

        data2 = data2.drop(columns=['Enter Row # for Each TEST Sample'])

        # join first block and second block of results
        result = pd.concat([data1, data2], axis=1, join="inner")
        result = result.loc[:, ~result.columns.duplicated()]
        # add new column which differentiates test and control results
        result = result.assign(family="test")

        ## Extract Control Results
        # exactly the same procedure as above
        data_s1 = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each Control Sample", na=False)
        ].tolist()
        data_s2 = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each CONTROL Sample", na=False)
        ].tolist()
        data1 = df_temp1[data_s1[0] : data_s2[0]]
        data1 = data1[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data1.columns = data1.iloc[0]
        data1 = data1.iloc[1:, :]
        data1.reset_index(drop=True, inplace=True)
        data1 = data1.dropna(subset=["Enter Row # for Each Control Sample"])
        try:
            data1 = data1[data1["Enter Row # for Each Control Sample"].str.lower() != "enter control here"]
        except:
            print('')
        data1 = data1.drop(columns=['Enter Row # for Each Control Sample'])

        data2 = df_temp1[data_s2[0] : data_s2[0] + 9]
        data2 = data2[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Enter Row # for Each CONTROL Sample"])
        try:
            data2 = data2[data2["Enter Row # for Each CONTROL Sample"].str.lower() != "enter control here"]
        except:
            print('')
        data2 = data2.drop(columns=['Enter Row # for Each CONTROL Sample'])

        control = pd.concat([data1, data2], axis=1, join="inner")
        control = control.loc[:, ~control.columns.duplicated()]
        control = control.assign(family="control")

        # Now add the control and test blocks
        data = result.append(control)
        # pivot the above dataframe to a long format
        data = data.melt(
            id_vars=["family", "Description"], var_name="metric"
        )
        # add meta data to the dataframe
        data = data.assign(wo = wo, filename=file, date=e_date, qc_by=qc_by)

        if len(data)  == 0:
            error_log.append("No Disposition Data Entered")

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking", header=None)
        
        # product tested for easy linking to trend chart
        product = df_ln[df_ln[1].str.contains("Single Cell Product", na=False)].iloc[0,2]

        data_s = df_ln.index[
            df_ln[1].str.contains("Single Cell Library reagents", na=False)
        ].tolist()
        data_e = df_ln.index[
            df_ln[1].str.contains("Index Kit T", na=False)
        ].tolist()
        lns = df_ln[data_s[0] : data_e[0]]
        # select relevant columns
        lns = lns[[0, 1, 2, 3, 4]]
        # set column names from first row
        lns.columns = lns.iloc[0]
        lns = lns.iloc[1:, :]
        lns.reset_index(drop=True, inplace=True)

        # rename columns
        lns.columns = ["subname","pn_descrip", "pn", "test", "control"]

        ## Temp code for adding subname
        ## delete after ECO
        # subname = ['','','','','','','',
        #           '','','','','','',
        #           '','','GEM Kit','','','',
        #           '','','','','Library Kit','']

        # lns['subname'] = subname

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

        # check if lots only have the 6 digit wo
        if any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['test']]) or any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['control']]):
            error_log.append("Lots entered on LN Tracking page not correct")

        # Pivot rows to long format
        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        # add file name (metadata) column
        lns = lns.assign(filename=file)

        # merge the lot information with previous metrics information
        # we are creating a staging table here
        # it will be normalized and stored in the DB using a stored procedure
        df = pd.merge(data, lns, on=["filename", "family"])

        # adding the product column previously extracted from the LN tracking tab
        df = df.assign(product = product, runnum = run_num)

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

def read_qc149_data_revF(file):

    # creating a list of errors, later to be mailed to the qc folks
    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary-QC records_SC5'v2", header=None)

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]
        #pn = df_temp[df_temp[1].str.contains("10X Part Number", na=False)].iloc[0, 2]

        # check if qc_by is empty
        if qc_by == "Enter Here":
            print("File not ready for ingestion")
            return pd.DataFrame()

        wo = df_temp[df_temp[1].str.contains("Work Order", na=False)].iloc[0, 2]

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
            try:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%b-%Y"), "%Y-%m-%d"
                )
            except:
                e_date = dt.date.strftime(
                    dt.datetime.strptime(date_string, "%d-%m-%Y"), "%Y-%m-%d"
                )
            # making this assumption since CA enters date in a particular format
            #file_loc = "CA"
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
            #file_loc = "SG"

        # if the WO starts with a '1' then set file_loc as "CA" if it starts with '2' then 
        # file_loc as "SG"
        if re.search("\d",str(wo)).group(0) == '1':
            file_loc = "CA"
        elif re.search("\d",str(wo)).group(0) == '2':
            file_loc = "SG"
        else:
            file_loc = "NA"
            error_log.append("'WO on summary page is wrong' does not start with 1 or 2")

        try:
            run_num = re.findall('[R|r]un.?(\d)', file)[0]
        except:
            run_num = 1
            #error_log.append("'Run Number not found' add '_run #' to file name")

        df_temp1 = pd.read_excel(xlsx, sheet_name="Disposition_SC5'v2", header=None)

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
        data = data.assign(wo = wo, filename=file, date=e_date, qc_by=qc_by)

        if len(data)  == 0:
            error_log.append("No Disposition Data Entered")

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking for 5'v2", header=None)

        # product tested for easy linking to trend chart
        product = "Next GEM SC5' v2"

        data_s = df_ln.index[
            df_ln[1].str.contains("Single Cell Library reagents", na=False)
        ].tolist()
        data_e = df_ln.index[
            df_ln[1].str.contains("Dual Index Kit T", na=False)
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

        ## Temp code for adding subname
        ## delete after ECO
        # subname = ['','','','','','','','',
        #            '','','','','','','',
        #            '','','','','','','',
        #            '','','','','','','','',
        #            '','','GEM Kit','','','','',
        #            '','','Library Kit','','','','',
        #            '','','','','','','',
        #            '','','','','','','']

        # lns['subname'] = subname

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

        # check if lots only have the 6 digit wo
        if any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['test']]) or any([bool(re.search("[^\d{6}\s\/\,:]", str(x))) for x in lns['control']]):
            error_log.append("Lots entered on LN Tracking page not correct")

        # Pivot rows to long format
        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        # add file name (metadata) column
        lns = lns.assign(filename=file)

        # merge the lot information with previous metrics information
        # we are creating a staging table here
        # it will be normalized and stored in the DB using a stored procedure
        df = pd.merge(data, lns, on=["filename", "family"])

        # adding the product column previously extracted from the LN tracking tab
        df = df.assign(product = product, runnum = run_num)

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

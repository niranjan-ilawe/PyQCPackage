import pandas as pd
import hashlib
import datetime as dt
import numpy as np
from pyqc.send_error_emails import send_error_emails


def read_funcseq_qc_data(file):

    error_log = ["Errors"]
    try:
        xlsx = pd.ExcelFile(file)

        df_temp = pd.read_excel(xlsx, sheet_name="Summary-QC records", header=None)

        date = dt.date.strftime(
            df_temp[df_temp[1].str.contains("QC date", na=False)].iloc[0, 2], "%Y-%m-%d"
        )

        qc_by = df_temp[df_temp[1].str.contains("QC'ed by", na=False)].iloc[0, 2]

        file_name = hashlib.sha1(file.encode("utf-8")).hexdigest()
        # run_number

        df_temp1 = pd.read_excel(xlsx, sheet_name="Disposition", header=None)
        ## Extract Test Results
        data_s = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each TEST Sample", na=False)
        ].tolist()
        data_e = df_temp1.index[
            df_temp1[0].str.contains("Control Results", na=False)
        ].tolist()
        data1 = df_temp1[data_s[0] : data_s[1]]
        data1 = data1[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data1.columns = data1.iloc[0]
        data1 = data1.iloc[1:, :]
        data1.reset_index(drop=True, inplace=True)
        data1 = data1.dropna(subset=["Sequencer"])

        data2 = df_temp1[data_s[1] : data_s[2]]
        data2 = data2[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Sequencer"])

        data3 = df_temp1[data_s[2] : data_e[0]]
        data3 = data3[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data3.columns = data3.iloc[0]
        data3 = data3.iloc[1:, :]
        data3.reset_index(drop=True, inplace=True)
        data3 = data3.dropna(subset=["Sequencer"])

        result = pd.concat([data1, data2, data3], axis=1, join="inner")
        result = result.loc[:, ~result.columns.duplicated()]
        result = result.assign(family="test")

        ## Extract Control Results
        data_s = df_temp1.index[
            df_temp1[0].str.contains("Enter Row # for Each CONTROL Sample", na=False)
        ].tolist()
        data_e = df_temp1.index[
            df_temp1[0].str.contains("Data Summary Chart", na=False)
        ].tolist()
        data1 = df_temp1[data_s[0] : data_s[1]]
        data1 = data1[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data1.columns = data1.iloc[0]
        data1 = data1.iloc[1:, :]
        data1.reset_index(drop=True, inplace=True)
        data1 = data1.dropna(subset=["Sequencer"])

        data2 = df_temp1[data_s[1] : data_s[2]]
        data2 = data2[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
        data2.columns = data2.iloc[0]
        data2 = data2.iloc[1:, :]
        data2.reset_index(drop=True, inplace=True)
        data2 = data2.dropna(subset=["Sequencer"])

        data3 = df_temp1[data_s[2] : data_e[0]]
        data3 = data3[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
        data3.columns = data3.iloc[0]
        data3 = data3.iloc[1:, :]
        data3.reset_index(drop=True, inplace=True)
        data3 = data3.dropna(subset=["Sequencer"])

        control = pd.concat([data1, data2, data3], axis=1, join="inner")
        control = control.loc[:, ~control.columns.duplicated()]
        control = control.assign(family="control")

        data = result.append(control)
        data = data.melt(
            id_vars=["family", "Sequencer", "Description"], var_name="metric"
        )
        data = data.assign(filename=file_name, date=date, qc_by=qc_by)

        # Extract lot info
        df_ln = pd.read_excel(xlsx, sheet_name="LN Tracking", header=None)
        data_s = df_ln.index[
            df_ln[0].str.contains("Single Cell Library reagents", na=False)
        ].tolist()
        data_e = df_ln.index[
            df_ln[0].str.contains("Chromium i7 Multiplex Kit", na=False)
        ].tolist()
        lns = df_ln[data_s[0] : data_e[0]]
        lns = lns[[0, 1, 2, 3]]
        lns.columns = lns.iloc[0]
        lns = lns.iloc[1:, :]
        lns.reset_index(drop=True, inplace=True)
        lns.columns = ["pn_descrip", "pn", "test", "control"]

        # drop rows that are not associated with any part number and description
        lns = lns.dropna(subset=["pn_descrip", "pn"])
        lns = lns.replace("Enter Here", np.nan)
        lns = lns.dropna(subset=["test", "control"], how="all")
        lns["check"] = np.where(
            (lns["test"].notnull()) & (lns["control"].notnull()),
            "Pass",
            "Fail",
        )

        if (lns["check"] == "Fail").any():
            raise NameError("The control and test lots not entered properly")

        lns = lns[lns["check"] == "Pass"]
        lns = lns.drop(columns=["check"])

        lns = lns.melt(id_vars=["pn_descrip", "pn"])
        lns.columns = ["pn_descrip", "pn", "family", "ln"]

        lns = lns.assign(filename=file_name)

        df = pd.merge(data, lns, on=["filename", "family"])

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
    except ValueError as err:
        if "Worksheet" in err.args[0]:
            msg = f"Summary-QC records, Disposition or LN Tracking sheet not found in {file}"
            error_log.append(msg)
    except TypeError as err:
        if "datetime.date" in err.args[0]:
            msg = f"QC date format incorrect in {file}"
            error_log.append(msg)
    except NameError as err:
        if "entered properly" in err.args[0]:
            msg = f"""The control and test lots not entered properly in {file}. 
                    Make sure control and test lots are entered appropriately."""
            error_log.append(msg)
    except:
        msg = f"Other unknown error in {file}. Please reach out to Process Dev Data Analytics."
        error_log.append(msg)

    if len(error_log) > 1:
        send_error_emails(filename=file, error_list=error_log, qc_by=qc_by)
        df = pd.DataFrame()

    return df

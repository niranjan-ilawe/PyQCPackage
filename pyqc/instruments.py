import ezsheets
import pandas as pd


def read_connect_google_sheet(sheet_id="1M7Jit8f9KST9U8PvH04dbqkjhHk3fiu-_Whk8j68qWw"):
    ss = ezsheets.Spreadsheet(sheet_id)

    sheet1 = ss["QC testing log"]

    d = {
        "qc_date": sheet1.getColumn(1),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(3),
        # "cosmetic_disp": sheet1.getColumn(6),
        # "functional_disp": sheet1.getColumn(7),
        "final_disp": sheet1.getColumn(8),
        "qc_attempt": sheet1.getColumn(9)
        # "second_sampling": sheet1.getColumn(9),
    }

    df_connect = pd.DataFrame(d)
    # drop first row
    df_connect = df_connect.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_connect.replace("", nan_value, inplace=True)
    df_connect = df_connect.dropna(subset=["sn"])
    df_connect = df_connect.assign(pn="Connect")

    return df_connect


def read_cac_google_sheet(sheet_id="1ou1P_jq6b3rcPaYebIpWIEBCH68OWd-ep-cophJ0pDA"):
    ss = ezsheets.Spreadsheet(sheet_id)

    sheet1 = ss["QC Log"]

    d = {
        "qc_date": sheet1.getColumn(2),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(4),
        "cosmetic_disp": sheet1.getColumn(6),
        "functional_disp": sheet1.getColumn(7),
        "final_disp": sheet1.getColumn(8),
        "qc_attempt": sheet1.getColumn(9),
    }

    df_cac = pd.DataFrame(d)
    # drop first row
    df_cac = df_cac.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_cac.replace("", nan_value, inplace=True)
    df_cac = df_cac.dropna(subset=["sn"])
    df_cac = df_cac.assign(pn="CAC")

    return df_cac


def read_controller_google_sheet(
    # sheet_id="1R6V3dcwMxP2zPcgaxmErL7vbCeb-5y2r6M7e_YI1dMA", 2021
    sheet_id="1q-Dw7mXEd8ZIlcePHw6KraZVnBT343wG1qSfC6t6Emo",
):

    ss = ezsheets.Spreadsheet(sheet_id)

    # sheet1 = ss["SG-QC Log"] 2021
    sheet1 = ss["Controller-QC Log"]

    d = {
        "qc_date": sheet1.getColumn(3),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(6),
        "cosmetic_disp": sheet1.getColumn(12),
        "functional_disp": sheet1.getColumn(13),
        "final_disp": sheet1.getColumn(14),
        "qc_attempt": sheet1.getColumn(15),
        "second_sampling": sheet1.getColumn(24),
    }

    df_ctrl21 = pd.DataFrame(d)
    # drop first row
    df_ctrl21 = df_ctrl21.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_ctrl21.replace("NA", nan_value, inplace=True)
    df_ctrl21.replace("", nan_value, inplace=True)
    df_ctrl21 = df_ctrl21.dropna(subset=["sn"])
    df_ctrl21["qc_date"] = df_ctrl21["qc_date"].fillna(method="ffill")
    df_ctrl21 = df_ctrl21.assign(pn="Controller")

    ## 2020 sheet
    sheet_id = "1R6V3dcwMxP2zPcgaxmErL7vbCeb-5y2r6M7e_YI1dMA"
    ss = ezsheets.Spreadsheet(sheet_id)

    sheet1 = ss["SG-QC Log"]

    d = {
        "qc_date": sheet1.getColumn(2),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(5),
        "cosmetic_disp": sheet1.getColumn(11),
        "functional_disp": sheet1.getColumn(12),
        "final_disp": sheet1.getColumn(13),
        "qc_attempt": sheet1.getColumn(14),
        "second_sampling": sheet1.getColumn(22),
    }

    df_ctrl20 = pd.DataFrame(d)
    # drop first row
    df_ctrl20 = df_ctrl20.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_ctrl20.replace("NA", nan_value, inplace=True)
    df_ctrl20.replace("", nan_value, inplace=True)
    df_ctrl20 = df_ctrl20.dropna(subset=["sn"])
    df_ctrl20["qc_date"] = df_ctrl20["qc_date"].fillna(method="ffill")
    df_ctrl20 = df_ctrl20.assign(pn="Controller")

    df_ctrl = df_ctrl20.append(df_ctrl21)

    return df_ctrl


def read_chromium_google_sheet(
    # sheet_id="1R6V3dcwMxP2zPcgaxmErL7vbCeb-5y2r6M7e_YI1dMA" 2021
    sheet_id="1q-Dw7mXEd8ZIlcePHw6KraZVnBT343wG1qSfC6t6Emo",
):

    ss = ezsheets.Spreadsheet(sheet_id)

    # sheet1 = ss["Chromium X -QC Log"] 2021
    sheet1 = ss["Chromium X-QC Log"]

    d = {
        "qc_date": sheet1.getColumn(3),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(5),
        "cosmetic_disp": sheet1.getColumn(12),
        "functional_disp": sheet1.getColumn(13),
        "final_disp": sheet1.getColumn(15),
        "qc_attempt": sheet1.getColumn(17)
        # "second_sampling": sheet1.getColumn(22),
    }

    df_chrm21 = pd.DataFrame(d)
    # drop first row
    df_chrm21 = df_chrm21.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_chrm21.replace("", nan_value, inplace=True)
    df_chrm21 = df_chrm21.dropna(subset=["sn"])
    df_chrm21 = df_chrm21.assign(pn="Chromium X")

    # 2020 sheet
    sheet_id = "1R6V3dcwMxP2zPcgaxmErL7vbCeb-5y2r6M7e_YI1dMA"
    ss = ezsheets.Spreadsheet(sheet_id)

    sheet1 = ss["Chromium X -QC Log"]

    d = {
        "qc_date": sheet1.getColumn(2),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(4),
        "cosmetic_disp": sheet1.getColumn(11),
        "functional_disp": sheet1.getColumn(12),
        "final_disp": sheet1.getColumn(13),
        "qc_attempt": sheet1.getColumn(14)
        # "second_sampling": sheet1.getColumn(22),
    }

    df_chrm20 = pd.DataFrame(d)
    # drop first row
    df_chrm20 = df_chrm20.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_chrm20.replace("", nan_value, inplace=True)
    df_chrm20 = df_chrm20.dropna(subset=["sn"])
    df_chrm20 = df_chrm20.assign(pn="Chromium X")

    df_chrm = df_chrm20.append(df_chrm21)

    return df_chrm

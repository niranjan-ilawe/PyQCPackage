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
        "qc_date": sheet1.getColumn(2),
        # "pn": sheet1.getColumn(4),
        "sn": sheet1.getColumn(5),
        "cosmetic_disp": sheet1.getColumn(11),
        "functional_disp": sheet1.getColumn(12),
        "final_disp": sheet1.getColumn(13),
        "qc_attempt": sheet1.getColumn(14),
        "second_sampling": sheet1.getColumn(22),
    }

    df_ctrl = pd.DataFrame(d)
    # drop first row
    df_ctrl = df_ctrl.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_ctrl.replace("NA", nan_value, inplace=True)
    df_ctrl.replace("", nan_value, inplace=True)
    df_ctrl = df_ctrl.dropna(subset=["sn"])
    df_ctrl["qc_date"] = df_ctrl["qc_date"].fillna(method="ffill")
    df_ctrl = df_ctrl.assign(pn="Controller")

    return df_ctrl


def read_chromium_google_sheet(
    # sheet_id="1R6V3dcwMxP2zPcgaxmErL7vbCeb-5y2r6M7e_YI1dMA" 2021
    sheet_id="1q-Dw7mXEd8ZIlcePHw6KraZVnBT343wG1qSfC6t6Emo",
):

    ss = ezsheets.Spreadsheet(sheet_id)

    # sheet1 = ss["Chromium X -QC Log"] 2021
    sheet1 = ss["Chromium X-QC Log"]

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

    df_chrm = pd.DataFrame(d)
    # drop first row
    df_chrm = df_chrm.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df_chrm.replace("", nan_value, inplace=True)
    df_chrm = df_chrm.dropna(subset=["sn"])
    df_chrm = df_chrm.assign(pn="Chromium X")

    return df_chrm

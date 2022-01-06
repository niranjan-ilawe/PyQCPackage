import ezsheets
import pandas as pd


def read_ca_google_sheet(sheet_id="1vuErPjswNyMVGQcBA8TewChb4R0GEurnRlqjehTFGw0"):

    ss = ezsheets.Spreadsheet(sheet_id)

    sheet1 = ss["QC Schedule"]

    d = {
        "pn": sheet1.getColumn(1),
        "descrip": sheet1.getColumn(2),
        "wo": sheet1.getColumn(3),
        "assay": sheet1.getColumn(4),
        "submit_date": sheet1.getColumn(6),
        "start_date": sheet1.getColumn(7),
        "end_date": sheet1.getColumn(8),
        "is_repeat": sheet1.getColumn(9),
        "status": sheet1.getColumn(12),
        "schedule": sheet1.getColumn(15),
        "clean_wo": sheet1.getColumn(17),
        "product": sheet1.getColumn(18),
        "pass_rate": sheet1.getColumn(19),
        "item_type": sheet1.getColumn(20),
        "final_testing": sheet1.getColumn(21),
        "kpi_family": sheet1.getColumn(22),
        "queue_time": sheet1.getColumn(23),
        "process_time": sheet1.getColumn(24),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["pn"])

    return df


def read_sg_google_sheet(sheet_id="17UufvY6Ub0Swb4KUwifVGtycwebUg1YzQdxmBaZnano"):

    ss = ezsheets.Spreadsheet(sheet_id)

    sheet1 = ss["QC Testing Planning"]

    d = {
        "pn": sheet1.getColumn(1),
        "descrip": sheet1.getColumn(3),
        "wo": sheet1.getColumn(2),
        "assay": sheet1.getColumn(4),
        "submit_date": sheet1.getColumn(5),
        "start_date": sheet1.getColumn(6),
        "end_date": sheet1.getColumn(7),
        "status": sheet1.getColumn(9),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]
    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["pn"])

    return df

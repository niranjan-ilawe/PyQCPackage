from pyqc.instruments import (
    read_cac_google_sheet,
    read_chromium_google_sheet,
    read_connect_google_sheet,
    read_controller_google_sheet,
)

from pyqc.consumables import read_ca_google_sheet, read_sg_google_sheet

from pydb import batch_upload_df, get_postgres_connection
from pygbmfg.common import _load_credentials, _clear_credentials


def run_instrument_qc_pipeline():

    print("****** Pipeline Starting ******")

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    _load_credentials()

    df = read_cac_google_sheet()
    print("-- Uploading CAC data")
    res = batch_upload_df(
        conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    )

    df = read_chromium_google_sheet()
    print("-- Uploading Chromium data")
    res = batch_upload_df(
        conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    )

    df = read_connect_google_sheet()
    print("-- Uploading Connect data")
    res = batch_upload_df(
        conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    )

    df = read_controller_google_sheet()
    print("-- Uploading Controller data")
    res = batch_upload_df(
        conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    )

    _clear_credentials()

    print("****** Pipeline Completed ******")


def run_consumables_qc_pipeline():

    print("****** Pipeline Starting ******")

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    _load_credentials()

    df = read_ca_google_sheet()
    print("-- Uploading CA QC data")
    res = batch_upload_df(
        conn=conn, df=df, tablename="yield.qc_data", insert_type="refresh"
    )

    df = read_sg_google_sheet()
    print("-- Uploading SG QC data")
    res = batch_upload_df(
        conn=conn, df=df, tablename="yield.qc_data_sg", insert_type="refresh"
    )

    _clear_credentials()

    print("****** Pipeline Completed ******")

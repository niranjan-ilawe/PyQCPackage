from termcolor import colored

from pyqc.instruments import (
    read_cac_google_sheet,
    read_chromium_google_sheet,
    read_connect_google_sheet,
    read_controller_google_sheet,
)

from pyqc.consumables import read_ca_google_sheet, read_sg_google_sheet

from pydb import batch_upload_df, get_postgres_connection
from pygbmfg.common import _load_credentials, _clear_credentials
from pyqc.material_qc.df_creation_scripts import get_tso_data

from pyqc.kit_qc.df_creation_scripts import get_funcseq_data


def run_instrument_qc_pipeline():

    print("****** Pipeline Starting ******")

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    _load_credentials()

    cac = read_cac_google_sheet()
    # print("-- Uploading CAC data")
    # res = batch_upload_df(
    #    conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    # )

    chromium = read_chromium_google_sheet()
    # print("-- Uploading Chromium data")
    # res = batch_upload_df(
    # conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    # )

    connect = read_connect_google_sheet()
    # print("-- Uploading Connect data")
    # res = batch_upload_df(
    # conn=conn, df=df, tablename="yield.instr_qc_data", insert_type="refresh"
    # )

    controller = read_controller_google_sheet()

    df = cac.append(chromium.append(connect.append(controller)))

    print("-- Uploading Instrument data")
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


def run_material_qc_pipeline(days=3):

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    print("---- Getting TSO QC Data ----")
    try:
        df = get_tso_data(days)
        print(colored("---- Uploading TSO QC Data ----", "green"))
        batch_upload_df(
            conn=conn,
            df=df,
            tablename="gbmfg.kit_funcseq_data",
            insert_type="update",
            key_cols="lot",
        )
    except:
        print(colored("---- Skipping TSO QC Data ----", "yellow"))


def run_qc123_pipeline(days=3):

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    print("---- Getting QC123 Data ----")
    try:
        df = get_funcseq_data(days)
        print(colored("---- Uploading QC123 Data ----", "green"))
        cur = conn.cursor()
        cur.execute(f"DELETE FROM kitqc.metrics_stg;")
        conn.commit()
        batch_upload_df(
            conn=conn,
            df=df,
            tablename="kitqc.metrics_stg",
            insert_type="refresh"
        )
        cur.execute(f"call kitqc.sp_upload_qc123_data(1)")
        conn.commit()
    except:
        print(colored("---- Skipping QC123 Data ----", "yellow"))

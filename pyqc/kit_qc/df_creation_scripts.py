from datetime import date, timedelta
from pybox import box_create_df_from_files, get_box_client

from pyqc.kit_qc.file_reading_scripts import read_qc123_data, read_qc167_data

from pyqc.common import _load_credentials, _clear_credentials


def get_qc123_data(days=3):

    _load_credentials()
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC3' kit data
    ca_sc3 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="112734413150",
        file_extension="xlsx",
        file_pattern="Rev N",
        file_parsing_functions=read_qc123_data,
    )

    if ca_sc3.shape[0] > 0:
        ca_sc3 = ca_sc3.assign(site="CA")

    ## Get CA SC3' kit data
    sg_sc3 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="137579882492",
        file_extension="xlsx",
        file_pattern="Rev N",
        file_parsing_functions=read_qc123_data,
    )

    if sg_sc3.shape[0] > 0:
        sg_sc3 = sg_sc3.assign(site="SG")

    df = ca_sc3.append(sg_sc3)

    _clear_credentials()
    return df

def get_qc167_data(days=3):

    _load_credentials()
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC3' kit data
    ca_sc3 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="137191976028",
        file_extension="xlsx",
        file_pattern="Rev B",
        file_parsing_functions=read_qc167_data,
    )

    if ca_sc3.shape[0] > 0:
        ca_sc3 = ca_sc3.assign(site="CA")

    ## Get CA SC5' kit data
    ca_sc5 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="140180957543",
        file_extension="xlsx",
        file_pattern="Rev B",
        file_parsing_functions=read_qc167_data,
    )

    if ca_sc5.shape[0] > 0:
        ca_sc5 = ca_sc5.assign(site="CA")

    df = ca_sc3.append(ca_sc5)
    _clear_credentials()
    return df

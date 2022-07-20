from datetime import date, timedelta
from pybox import box_create_df_from_files, get_box_client

from pyqc.kit_qc.file_reading_scripts import read_qc123_data_revN, read_qc167_data_revB, read_qc123_data_revP, read_qc167_data_revC

from pyqc.common import _load_credentials, _clear_credentials


def get_qc123_data(days=3):

    _load_credentials()
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC3' kit data
    # March 2020 - Present/1000122, 094, 158, 123, 157, 120, 144, 127 (SC3_ v3.1 Kits)
    ca_sc3_1 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="112734413150",
        file_extension="xlsx",
        file_pattern="Rev N",
        file_parsing_functions=read_qc123_data_revN,
    )

    if ca_sc3_1.shape[0] > 0:
        ca_sc3_1 = ca_sc3_1.assign(site="CA")

    ca_sc3_2 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="112734413150",
        file_extension="xlsx",
        file_pattern="Rev P",
        file_parsing_functions=read_qc123_data_revP,
    )

    if ca_sc3_2.shape[0] > 0:
        ca_sc3_2 = ca_sc3_2.assign(site="CA")

    ## Get CA SC3' kit data
    # SG QC Data/ 1000094, 122, 123, 130, 144, 157, 158 (SC3_ v3.1 Kits)
    sg_sc3_1 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="137579882492",
        file_extension="xlsx",
        file_pattern="Rev N",
        file_parsing_functions=read_qc123_data_revN,
    )

    if sg_sc3_1.shape[0] > 0:
        sg_sc3_1 = sg_sc3_1.assign(site="SG")

    sg_sc3_2 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="137579882492",
        file_extension="xlsx",
        file_pattern="Rev P",
        file_parsing_functions=read_qc123_data_revP,
    )

    if sg_sc3_2.shape[0] > 0:
        sg_sc3_2 = sg_sc3_2.assign(site="SG")

    df = ca_sc3_1.append(ca_sc3_2.append(sg_sc3_1.append(sg_sc3_2)))

    _clear_credentials()
    return df

def get_qc167_data(days=3):

    _load_credentials()
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC3' kit data
    # March 2020 - Present/1000349, 1000351, 1000373, 2000443 (HT SC3'v3.1)
    ca_sc3_1 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="137191976028",
        file_extension="xlsx",
        file_pattern="Rev B",
        file_parsing_functions=read_qc167_data_revB,
    )

    if ca_sc3_1.shape[0] > 0:
        ca_sc3_1 = ca_sc3_1.assign(site="CA")

    ca_sc3_2 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="137191976028",
        file_extension="xlsx",
        file_pattern="Rev C",
        file_parsing_functions=read_qc167_data_revC,
    )

    if ca_sc3_2.shape[0] > 0:
        ca_sc3_2 = ca_sc3_2.assign(site="CA")

    ## Get CA SC5' kit data
    # March 2020 - Present/1000357, 2000444, 1000359, 1000375, 1000377 (HT SC5'v2)
    ca_sc5_1 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="140180957543",
        file_extension="xlsx",
        file_pattern="Rev B",
        file_parsing_functions=read_qc167_data_revB,
    )

    if ca_sc5_1.shape[0] > 0:
        ca_sc5_1 = ca_sc5_1.assign(site="CA")

    ca_sc5_2 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="140180957543",
        file_extension="xlsx",
        file_pattern="Rev C",
        file_parsing_functions=read_qc167_data_revC,
    )

    if ca_sc5_2.shape[0] > 0:
        ca_sc5_2 = ca_sc5_2.assign(site="CA")

    df = ca_sc3_1.append(ca_sc3_2.append(ca_sc5_1.append(ca_sc5_2)))
    _clear_credentials()
    return df

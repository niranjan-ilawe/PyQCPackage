from datetime import date, timedelta
from pybox import box_create_df_from_files, get_box_client

from pyqc.kit_qc.file_reading_scripts import read_qc123_data_revN, read_qc167_data_revB, read_qc123_data_revP, read_qc167_data_revC, read_qc149_data_revF, read_qc123_data_revK

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

def get_qc149_data(days=3):

    _load_credentials()
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC5' kit data
    # March 2020 - Present/1000244, 1000266, 1000286, 2000209 (SC5' GEM Kit v2, Chip K, v2 GB)
    ca_sc5 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="114259185535",
        file_extension="xlsx",
        file_pattern="Rev F",
        file_parsing_functions=read_qc149_data_revF
    )

    if ca_sc5.shape[0] > 0:
        ca_sc5 = ca_sc5.assign(site="CA")

    ## Get SG SC5' kit data
    # SG QC Data/1000264, 267 (SC5' GB Kit)
    sg_sc5 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="149228044522",
        file_extension="xlsx",
        file_pattern="Rev F",
        file_parsing_functions=read_qc149_data_revF
    )

    if sg_sc5.shape[0] > 0:
        sg_sc5 = sg_sc5.assign(site="SG")

    df = ca_sc5.append(sg_sc5)

    _clear_credentials()
    return df

def get_historical_data(days=3):

    _load_credentials()
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC3' kit data
    # March 2020 - Present/1000122, 094, 158, 123, 157, 120, 144, 127 (SC3_ v3.1 Kits)
    revk_data = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="168535507101",
        file_extension="xlsx",
        file_pattern="Rev",
        file_parsing_functions=read_qc123_data_revK,
    )

    revj_data = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="168535507101",
        file_extension="xlsx",
        file_pattern="Rev J",
        file_parsing_functions=read_qc123_data_revK,
    )

    revk_data = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="168535507101",
        file_extension="xlsx",
        file_pattern="Rev K",
        file_parsing_functions=read_qc123_data_revK,
    )

    revk_data = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="168535507101",
        file_extension="xlsx",
        file_pattern="Rev K",
        file_parsing_functions=read_qc123_data_revK,
    )

    df = revk_data.append(revj_data)

    _clear_credentials()
    return df

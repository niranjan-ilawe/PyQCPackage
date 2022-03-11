from datetime import date, timedelta
from pybox import box_create_df_from_files, get_box_client

from pyqc.material_qc.file_reading_scripts import read_tso_qc_data


def get_tso_data(days=3):

    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    ## Get CA SC3' kit data
    ca_sc3 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="112746574343",
        file_extension="xlsx",
        file_pattern="Template Switch",
        file_parsing_functions=read_tso_qc_data,
    )

    if ca_sc3.shape[0] > 0:
        ca_sc3 = ca_sc3.assign(site="CA")

    ## Get CA SC3' kit data
    # sg_sc3 = box_create_df_from_files(
    #    box_client=client,
    #    last_modified_date=last_modified_date,
    #    box_folder_id="137579882492",
    #    file_extension="xlsx",
    #    file_pattern="Sequencing",
    #    file_parsing_functions=read_funcseq_qc_data,
    # )

    # if sg_sc3.shape[0] > 0:
    #    sg_sc3 = sg_sc3.assign(site="SG")

    # df = ca_sc3.append(sg_sc3)
    df = ca_sc3

    return df

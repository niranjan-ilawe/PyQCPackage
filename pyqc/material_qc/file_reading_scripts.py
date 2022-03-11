import pandas as pd


def read_tso_qc_data(file):
    try:
        xlsx = pd.ExcelFile(file)
        try:
            df_temp = pd.read_excel(xlsx, sheet_name="Summary-QC Record", header=None)
            pn = df_temp[df_temp[1].str.contains("Part #", na=False)].iloc[0, 4]
            lot = df_temp[df_temp[1].str.contains("Lot #", na=False)].iloc[0, 4]
            wo = "NULL"
            date = df_temp[df_temp[1].str.contains("QC dates", na=False)].iloc[0, 4]
        except:
            print(f"Error in Summary section of {file}")
        try:
            df_temp1 = pd.read_excel(xlsx, sheet_name="Disposition", header=None)
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

            data3 = df_temp1[data_s[2] : data_s[3]]
            data3 = data3[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
            data3.columns = data3.iloc[0]
            data3 = data3.iloc[1:, :]
            data3.reset_index(drop=True, inplace=True)
            data3 = data3.dropna(subset=["Sequencer"])

            result1 = pd.concat([data1, data2, data3], axis=1, join="inner")
            result1 = result1.loc[:, ~result1.columns.duplicated()]

            ###--- Test tube 2
            data1 = df_temp1[data_s[3] : data_s[4]]
            data1 = data1[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
            data1.columns = data1.iloc[0]
            data1 = data1.iloc[1:, :]
            data1.reset_index(drop=True, inplace=True)
            data1 = data1.dropna(subset=["Sequencer"])

            data2 = df_temp1[data_s[4] : data_s[5]]
            data2 = data2[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
            data2.columns = data2.iloc[0]
            data2 = data2.iloc[1:, :]
            data2.reset_index(drop=True, inplace=True)
            data2 = data2.dropna(subset=["Sequencer"])

            data3 = df_temp1[data_s[5] : data_s[6]]
            data3 = data3[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
            data3.columns = data3.iloc[0]
            data3 = data3.iloc[1:, :]
            data3.reset_index(drop=True, inplace=True)
            data3 = data3.dropna(subset=["Sequencer"])

            result2 = pd.concat([data1, data2, data3], axis=1, join="inner")
            result2 = result2.loc[:, ~result2.columns.duplicated()]

            ## ---- Test tube 3
            data1 = df_temp1[data_s[6] : data_s[7]]
            data1 = data1[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
            data1.columns = data1.iloc[0]
            data1 = data1.iloc[1:, :]
            data1.reset_index(drop=True, inplace=True)
            data1 = data1.dropna(subset=["Sequencer"])

            data2 = df_temp1[data_s[7] : data_s[8]]
            data2 = data2[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
            data2.columns = data2.iloc[0]
            data2 = data2.iloc[1:, :]
            data2.reset_index(drop=True, inplace=True)
            data2 = data2.dropna(subset=["Sequencer"])

            data3 = df_temp1[data_s[8] : data_e[0]]
            data3 = data3[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]
            data3.columns = data3.iloc[0]
            data3 = data3.iloc[1:, :]
            data3.reset_index(drop=True, inplace=True)
            data3 = data3.dropna(subset=["Sequencer"])

            result3 = pd.concat([data1, data2, data3], axis=1, join="inner")
            result3 = result3.loc[:, ~result3.columns.duplicated()]
        except:
            print(f"Error in Disposition section of {file}")

        result = result1.append(result2.append(result3))
        data = result.assign(pn=pn, wo=wo, lot=lot, date=date)

        data.columns = (
            data.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace(".", "")
            .str.replace("<", "lessthan")
            .str.replace(">", "greaterthan")
        )

        data = data.melt(
            id_vars=["pn", "lot", "wo", "date", "description", "sequencer"],
            var_name="data_name",
            value_name="data_value",
        )

    except:
        print(f"{file} could not be processed")
        data = pd.DataFrame()

    return data

import os
import logging
from datetime import date
import pandas as pd

# Включаю логирование сессий
logging.basicConfig(
    filename="app.log",
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


# загрузка файлов
def newest(path: str) -> str:
    """
    возвращает последний файл в папке

    """
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)


def load_data() -> pd.DataFrame:
    """
    Выгружает данные из .csv файла full_k.csv

    """
    last_updated_file_path = newest(path="./datasets/main_data")
    df = pd.read_csv(last_updated_file_path, dtype=str)

    # явная присвоение типов данных
    df["Oтчетная дата"] = pd.to_datetime(df["Oтчетная дата"])
    df["Дата начала"] = pd.to_datetime(df["Дата начала"])
    df["Дефолт"] = df["Дефолт"].astype(int)
    return df


def ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Достаем ID клиентов, поскольку id есть не во всех ячейках был построен костыль
    """
    df["Дата начала"] = (
        df["Дата начала"]
        + pd.offsets.MonthEnd(0)
        - pd.offsets.MonthBegin(normalize=True)
    )
    df = df.melt(id_vars="Дата начала", value_vars="ID")
    df = df.rename(columns={"Дата начала": "date", "value": "id"})[["date", "id"]]
    df = df.sort_values("date").drop_duplicates("id", keep="first")
    df.date = df.date + pd.DateOffset(months=1)
    df = ids_kik(df)
    return df


def ids_kik(df: pd.DataFrame, data_of_start: str = "2021-05-01") -> pd.DataFrame:
    """
    костыль так как до определенной даты ID не указывался
    """
    df = df[df.date >= data_of_start]
    df_s = pd.read_csv("./datasets/ID_old.csv", dtype=str)
    df_s["date"] = pd.to_datetime(df_s["date"])
    df_s = df_s[df_s["date"] < data_of_start]
    df_s = df_s.sort_values("date").drop_duplicates("id", keep="first")
    df = pd.concat([df_s, df])
    return df


def vintage(df: pd.DataFrame, ids: pd.DataFrame) -> pd.DataFrame:
    """
    формирование винтажа
    """
    date_range = pd.date_range(
        start=df["Oтчетная дата"].max() - pd.DateOffset(years=5),
        end=df["Oтчетная дата"].max(),
        freq="MS",
    )

    vintage = []
    for date in date_range:
        id_list = ids[ids["date"] == date]["id"].drop_duplicates().values
        temp = (
            df[(df["ID"].isin(id_list)) & (df["Дефолт"] >= 90) & (df["Дефолт"] <= 120)]
            .groupby("Oтчетная дата")
            .count()["Дефолт"]
            .reset_index()
        )
        temp.rename(columns={"Дефолт": date.date()}, inplace=True)

        date_range = pd.date_range(start=date, end=df["Oтчетная дата"].max(), freq="MS")
        merge_month = pd.DataFrame(
            {
                "Oтчетная дата": date_range,
                "months": [i for i in range(1, len(date_range) + 1)],
            }
        )
        temp = pd.merge(merge_month, temp, on="Oтчетная дата", how="left")
        temp.drop("Oтчетная дата", axis=1, inplace=True)
        temp = temp.fillna("-")
        vintage.append(temp)

        # print(date)

    df = pd.concat(vintage, axis=1).drop("months", axis=1)
    df.index = df.index + 1
    df = df.transpose().loc[:, 3:]

    df.index = pd.to_datetime(df.index)

    df = pd.merge(
        df,
        ids.date.value_counts().sort_index(),
        left_on=df.index,
        right_on=ids.date.value_counts().sort_index().index,
    )
    df = df.rename(columns={"key_0": "отчетная дата", "date": "Количество"})
    df = df[["отчетная дата", "Количество"] + [i for i in range(3, 61)]]

    date_range = pd.DataFrame({"отчетная дата": date_range})
    df = pd.merge(df, date_range, how="outer")
    df["Количество"] = df["Количество"].fillna(0)
    df["отчетная дата"] = pd.to_datetime(df["отчетная дата"], format="%d.%m.%y")
    return df


def upload_new_data(new_file_path) -> pd.DataFrame:
    """
    Загрузка и принятие нового отчета
    """
    data = []
    loggs = {
        "коментарии": [],
        "кол-во записей": [],
        "отчетная дата": [],
    }

    file = pd.ExcelFile(new_file_path)
    sheets = file.sheet_names
    sheets
    if len(sheets) > 1:
        logging.error("Кол-во страниц превышено")
    elif (len(sheets[0]) != 8) or (sheets[0][:2] != "01"):
        logging.error("неправильное название страницы (01.мм.гг)")
    else:
        df = pd.read_excel(new_file_path, sheet_name=sheets[0], dtype=str)
        # print(df.iloc[0,0])
        loggs["коментарии"].append(df.iloc[0, 0])
        df.columns = df.iloc[2]
        df = df.iloc[3:]
        df = df[df["ID"].notna()]

        # print(len(df.columns))
        loggs["кол-во записей"].append(len(df.columns))
        loggs["отчетная дата"].append(sheets[0])
        df = df[
            [
                "ID",
                "Дата начала",
                "Кол-во дней просрочки по ОД",
                "Кол-во дней просрочки по НВ",
                "Кол-во дней просрочки по гарантийному взносу",
            ]
        ]

        try:
            df["ID"].astype(float)
        except:
            logging.error("в столбце ID есть не численные значения")
        df["ID"] = df["ID"].astype(str)
        try:
            pd.to_datetime(df["Дата начала"])
        except:
            logging.error("некорректное заполнение столбца 'Дата начала' ")
        try:
            df["Кол-во дней просрочки по ОД"] = df[
                "Кол-во дней просрочки по ОД"
            ].astype(float)
            df["Кол-во дней просрочки по НВ"] = df[
                "Кол-во дней просрочки по НВ"
            ].astype(float)
            df["Кол-во дней просрочки по гарантийному взносу"] = df[
                "Кол-во дней просрочки по гарантийному взносу"
            ].astype(float)
        except:
            logging.error("некорректное заполнение столбцов прострочек")

        df["Дефолт"] = (
            df[
                [
                    "Кол-во дней просрочки по ОД",
                    "Кол-во дней просрочки по НВ",
                    "Кол-во дней просрочки по гарантийному взносу",
                ]
            ]
            .max(axis=1)
            .astype(int)
        )
        df["Oтчетная дата"] = sheets[0]
        df["Oтчетная дата"] = pd.to_datetime(df["Oтчетная дата"], format="%d.%m.%y")
        df.drop(
            [
                "Кол-во дней просрочки по ОД",
                "Кол-во дней просрочки по НВ",
                "Кол-во дней просрочки по гарантийному взносу",
            ],
            axis=1,
            inplace=True,
        )
        df.reset_index(drop=True, inplace=True)

        loggs = pd.DataFrame(loggs)
        loggs["last_update"] = date.today()

        pd.concat([pd.read_csv("./datasets/loggs_k.csv"), loggs]).to_csv(
            "./datasets/loggs_k.csv", index=False
        )
        return df


def add_merge(path: str) -> None:
    """
    Функция для замены или дополнения основного датасета

    """

    df_main = load_data()

    try:
        df_add_or_replace = upload_new_data(path)
    except:
        logging.error("Инициализация файла не прошла успешно")

    if df_add_or_replace["Oтчетная дата"].iloc[0] > df_main["Oтчетная дата"].max():
        logging.info("Добавление нового отчета")
        df = pd.concat([df_main, df_add_or_replace], axis=0)
        df.to_csv(f"./datasets/main_data/full_k_{date.today()}.csv", index=False)
    else:
        logging.info("Замена отчета")
        df_main = df_main[
            df_main["Oтчетная дата"] != df_add_or_replace["Oтчетная дата"].iloc[0]
        ]
        df = pd.concat([df_main, df_add_or_replace], axis=0)
        df.to_csv(f"./datasets/main_data/full_k_{date.today()}.csv", index=False)


if __name__ == "__main__":

    logging.info("запуск __main__")

    ##
    #
    # add_merge(new_file_path)
    #
    ##
    df = load_data()
    ids = ids(df)
    vintage(df, ids).to_excel(
        "./datasets/vintage/vintage_кик_" + str(date.today()) + ".xlsx"
    )
#

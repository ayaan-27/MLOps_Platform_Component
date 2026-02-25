from typing import Any, Dict

import pandas as pd
from faker import Faker

fake = Faker(["en_IN"])
Faker.seed(4321)
di = {}


def mask_(list_words):
    list2 = []
    for word in list_words:
        word = str(word)
        if word in di:
            value = di[word]
            list2.append(value)
        else:
            value = fake.pystr(min_chars=None, max_chars=len(str(word)))
            di[word] = value
            list2.append(value)
    return list2


def pii(data: pd.DataFrame, col: str) -> pd.DataFrame:
    df_columns = data[col]
    data = data.drop(col, axis=1)
    rows_new = []
    for ind, rows in df_columns.iterrows():
        list_words = rows.to_list()
        masked_list = mask_(list_words)
        rows_new.append(masked_list)

    df_out = pd.DataFrame(rows_new, columns=df_columns.columns)
    res = pd.concat([data, df_out], axis=1)
    return res

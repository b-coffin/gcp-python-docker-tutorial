import json
import os
import polars
import re

COLOR_BLUE = "blue"
COLOR_YELLOW = "yellow"
COLOR_GREEN = "green"

def print_with_color(text: str, color: str) -> None:
    if color == COLOR_BLUE:
        print(f"\033[34m{text}\033[0m")
    elif color == COLOR_GREEN:
        print(f"\033[32m{text}\033[0m")
    elif color == COLOR_YELLOW:
        print(f"\033[33m{text}\033[0m")
    else:
        print(text)


# Terminal上で必要になるエスケープ処理を入れる
def get_escapedtext_forcommand(text: str) -> str:
    return text.replace(" ", "\\ ").replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)").replace("&", "\\&")


def get_filename_withoutextension(path: str) -> str:
    return os.path.splitext(os.path.basename(path))[0]


# 正規表現

def is_contain_allwords(text: str, words: list[str]) -> bool: # type: ignore
    """
    list形式で渡された文字列全てを含んでいるかどうかを判定
    """
    return True if re.search(rf"^{''.join(list(map(lambda x: f'(?=.*{x})', words)))}.*$", text) else False


# ファイルの書き込み

def write_jsonl_file(path: str, jsons: list[dict]) -> None: # type: ignore
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for j in jsons:
            f.writelines(f"{json.dumps(j)}\n")


def write_text_file(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def write_df_to_csv(path: str, df: polars.DataFrame) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.write_csv(path, separator=",")

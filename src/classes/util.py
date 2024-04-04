import json
import os

# Terminal上で必要になるエスケープ処理を入れる
def get_escapedtext_forcommand(text: str) -> str:
    return text.replace(" ", "\\ ").replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)").replace("&", "\\&")


def get_filename_withoutextension(path: str) -> str:
    return os.path.splitext(os.path.basename(path))[0]


# jsonlファイルの書き込み
def write_jsonl_file(path: str, jsons: list[dict]) -> None: # type: ignore
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for j in jsons:
            f.writelines(f"{json.dumps(j)}\n")


# テキストファイルの書き込み
def write_text_file(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)
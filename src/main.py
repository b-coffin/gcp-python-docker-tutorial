import datetime
import json
import os
import shutil
import traceback
from zoneinfo import ZoneInfo

from classes.config import Config
from classes.util import *

from bq.compare import bq_compare
from bq.select import bq_select
from storage.get import storage_get
from storage.upload import storage_upload

def main():

    # configファイルをキーボード入力から取得
    default_config_file = "sample_bq_getcolumn.json"
    config_filepath = os.path.join("config", input(f"Input config file name 1 (default: {default_config_file}) : ") or default_config_file)
    config_json = json.load(open(config_filepath, "r"))

    # configのバリデーションチェック
    try:
        config = Config(config_json)
    except Exception:
        print(f"Stacktrace: {traceback.format_exc()}")

    # 結果を格納するフォルダ
    result_dir = os.path.join("result", datetime.datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y%m%d-%H%M%S") + "_" + config.service)

    # configファイルをresult_dirに複製
    os.makedirs(result_dir, exist_ok=True)  # 結果フォルダを作成
    shutil.copy(config_filepath, os.path.join(result_dir, os.path.basename(config_filepath)))

    if config.service == Config.SERVICE_BQ:

        if config.mode == "compare":
            bq_compare(config, result_dir)
        elif config.mode == "select":
            bq_select(config, result_dir)

    elif config.service == Config.SERVICE_STORAGE:
            
        if config.mode == "get":
            storage_get(config, result_dir)
        elif config.mode == "upload":
            storage_upload(config)

    return

# メイン
if __name__ == "__main__":
    main()

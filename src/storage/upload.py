import os
import pathlib

from classes.config import Config
from classes.storage import Storage

def storage_upload(config: Config) -> None:
    """
    Google Cloud Storageにファイルをアップロードします。

    Parameters
    ----------
    config (Config): 設定情報

    Returns
    ----------
    None
    """
    storage: Storage = Storage(config)

    for path in [p for p in pathlib.Path().glob(f"input/{config.source_folder}/**/*") if p.is_file()]:
        storage.upload_blob(config.bucket, path, os.path.join(config.destination_folder, os.path.basename(path)))

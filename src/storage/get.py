from classes.config import Config
from classes.storage import Storage
from classes.util import *

def storage_get(config: Config, result_dir: str) -> None:
    """
    Google Cloud Storage の情報を取得します。

    Parameters
    ----------
    config (Config): 設定情報。
    result_dir (str): 結果を格納するフォルダ。

    Returns
    ----------
    None
    """
    storage: Storage = Storage(config)

    # サービスアカウントの認証情報を使用して署名付きURLを取得
    if config.blob_name and config.key_file:
        content_type = "application/octet-stream"
        write_used_jinja2template(
            os.path.join(os.path.dirname(__file__), "templates", "upload_by_signedurl.sh"),
            os.path.join(result_dir, "upload_by_signedurl.sh"),
            {
                "content_type": content_type,
                "url": storage.generate_upload_signed_url_v4(config.bucket, config.blob_name, content_type)
            }
        )

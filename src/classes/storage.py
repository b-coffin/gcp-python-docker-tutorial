import datetime
import pandas

from google.cloud import storage

from classes.config import Config
from classes.util import *

class Storage:

    def __init__(self, config: Config) -> None:
        if config.key_file:
            self.client = storage.Client.from_service_account_json(config.key_file)
        else:
            self.client = storage.Client(project=config.project)


    def upload_blob(self, bucket_name: str, source_file_name: str, destination_blob_name: str) -> None:
        """
        指定したGoogle Cloud Storageバケットにファイルをアップロードします。

        Parameters
        ----------
        bucket_name (str): アップロード先のバケット名。
        source_file_name (str): アップロードするファイルのパス。
        destination_blob_name (str): バケット内でのblob名。

        Returns
        ----------
        なし

        Example
        ----------
        >>> storage = Storage()
        >>> storage.upload_blob('my-bucket', '/path/to/my/file', 'my-file')
        ファイル /path/to/my/file を my-file にアップロードしました。
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(f"ファイル {source_file_name} を {destination_blob_name} にアップロードしました。")


    def upload_df(self, bucket_name: str, df: pandas.DataFrame, destination_blob_name: str) -> None:
        """
        指定した Google Cloud Storage バケットにデータフレームをアップロードします。

        Parameters
        ----------
        bucket_name (str): アップロード先のバケット名。
        df (pandas.DataFrame): アップロードするデータフレーム。
        destination_blob_name (str): バケット内でのblob名。

        Returns
        ----------
        なし

        Example
        ----------
        >>> storage = Storage()
        >>> storage.upload_df('my-bucket', df, 'my-file')
        データフレームを my-file にアップロードしました。
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

        print(f"データフレームを {destination_blob_name} にアップロードしました。")


    def generate_upload_signed_url_v4(self, bucket_name: str, blob_name: str, content_type: str = "application/octet-stream") -> str:
        """
        指定したバケットとblobに対して署名付きURLを生成します。

        Parameters
        ----------
        bucket_name (str): アップロード先のバケット名。
        blob_name (str): バケット内でのblob名。
        content_type (str): アップロードするファイルのContent-Type。

        Returns
        ----------
        url (str): 署名付きURL。
        
        Example
        ----------
        >>> storage = Storage()
        >>> url = storage.generate_upload_signed_url_v4('my-bucket', 'my-file')
        署名付きURLを発行しました。
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        url = blob.generate_signed_url(
            version='v4',
            expiration=datetime.timedelta(days=7),
            method='PUT',
            content_type=content_type,
        )

        print_with_color(f"署名付きURLを発行しました。", COLOR_GREEN)

        return url
    
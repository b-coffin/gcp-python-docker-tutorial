from google.cloud import storage

class Storage:

    def __init__(self, project: str) -> None:
        self.client = storage.Client(project=project)

    # バケット名、アップロード元のファイル名、アップロード先のファイル名を指定してアップロードを行う
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

# GCP Python

## 概要
GCPをpythonで操作

## 認証情報の設定
GCPのライブラリを使用するには、認証情報をダウンロードする必要がある。

[こちら](https://cloud.google.com/bigquery/docs/reference/libraries?hl=ja#setting_up_authentication)を参考に、認証情報の入ったJSONファイルをダウンロードする。

makeコマンドで実行する場合は下記

```
make get-gcloud-auth-json
```

## VSCodeの Remote Container を使用する場合
.devcontainerフォルダのあるディレクトリ（本READMEのあるディレクトリ）をVSCodeで開き、左下の「Open a Remote Window」>「Reopen in Container」をクリックすることで利用できる。

### 参考
* [Dev Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
* [VSCodeリモートコンテナで、簡単にPythonの環境構築をする方法 | Qiita](https://qiita.com/Yu_Mochi/items/68f033e9364b8379ed49)

## エラー対処
### google.auth.exceptions.RefreshError: ('invalid_grant: Bad Request', {'error': 'invalid_grant', 'error_description': 'Bad Request'}
もう一度下記コマンドを実行して認証情報をダウンロードする

```
make get-gcloud-auth-json
```

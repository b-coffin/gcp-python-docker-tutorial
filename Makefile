# gcloudの認証ファイル取得
# 参考: https://cloud.google.com/docs/authentication/application-default-credentials?hl=ja#personal
# 参考: https://blog.pokutuna.com/entry/gcloud-without-current-project
get-gcloud-auth-json:
	gcloud auth application-default login --disable-quota-project

# 上記で取得したクレデンシャルファイルの中身を表示
cat-adc:
	cat ~/.config/gcloud/application_default_credentials.json

# Docker

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down --rmi all --volumes

docker-exec:
	docker compose exec app bash

docker-exec-ls:
	docker compose exec app ls

# python

python-version:
	docker compose exec app python --version

# pip

pip-version:
	docker compose exec app pip --version

# python実行

main:
	docker compose exec app python main.py

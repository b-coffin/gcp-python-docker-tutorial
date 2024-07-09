curl -X PUT \
    -H 'Content-Type: {{ content_type }}' \
    --upload-file /path/to/my/file \
    '{{ url }}'

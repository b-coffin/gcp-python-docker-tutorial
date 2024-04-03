from google.cloud import bigquery

class Bigquery:

    def __init__(self, project):
        self.client = bigquery.Client(project = project)


    def get_schemafields(self, full_tableid):
        return self.client.get_table(full_tableid).schema


    # STRUCT構造になっている場合は `親カラム名.子カラム名` の形式でカラム名を返す
    def get_columnslist(self, full_tableid: str) -> list[str]: # type: ignore 

        # 再帰関数
        def yield_columnname(schemafields: list[bigquery.SchemaField], prefix: str|None = None) -> str: # type: ignore
            for schemafield in schemafields:
                fieldname_withprefix: str = f"{prefix + '.' if prefix else ''}{schemafield.name}"
                if schemafield.field_type == "RECORD":
                    for i in yield_columnname(schemafield.fields, fieldname_withprefix):
                        yield i                
                else:
                    yield fieldname_withprefix
        
        return list(yield_columnname(self.get_schemafields(full_tableid)))


    # 再帰関数
    def get_columnsjson(self, schemafields: list[bigquery.SchemaField], data: dict[str, str]|None, prefix: str|None = None) -> dict[str, str|list|dict]: # type: ignore
        result_columnsjson: dict = {}
        for schemafield in schemafields:
            fieldname_withprefix: str = f"{prefix + '.' if prefix else ''}{schemafield.name}"
            if schemafield.field_type == "RECORD":
                value = self.get_columnsjson(schemafield.fields, data, fieldname_withprefix)
                if schemafield.mode == "REPEATED":
                    result_columnsjson[schemafield.name] = [value]
                else:
                    result_columnsjson[schemafield.name] = value
            else:
                value = data[fieldname_withprefix] if data else self.get_defaultvalue(schemafield.field_type)
                if schemafield.mode == "REPEATED":
                    result_columnsjson[schemafield.name] = [value]
                else:
                    result_columnsjson[schemafield.name] = value

        return result_columnsjson


    def get_defaultvalue(self, field_type: str):
        if field_type in ["NUMERIC", "INTEGER"]:
            return 0
        elif field_type in ["DATE"]:
            return "9999-12-31"
        elif field_type in ["BOOLEAN"]:
            return "TRUE"
        else:
            return ""

from google.cloud import bigquery

class Bigquery:

    def __init__(self, project):
        self.client = bigquery.Client(project = project)


    def get_schemafields(self, full_tableid):
        return self.client.get_table(full_tableid).schema


    # STRUCT構造になっている場合は `親カラム名.子カラム名` の形式でカラム名を返す
    def get_columnslist(self, full_tableid: str) -> list[str]: # type: ignore 

        # 再帰関数
        def yield_columnname(schemafields: list[bigquery.SchemaField], prefix: str|None=None) -> str: # type: ignore
            for schemafield in schemafields:
                fieldname_withprefix: str = f"{prefix + '.' if prefix else ''}{schemafield.name}"
                if schemafield.field_type == "RECORD":
                    for i in yield_columnname(schemafield.fields, fieldname_withprefix):
                        yield i                
                else:
                    yield fieldname_withprefix
        
        return list(yield_columnname(self.get_schemafields(full_tableid)))


    def get_columnsjsonl(self, full_tableid):
        columnsjsonl = {}
        for schemafield in self.get_schemafields(full_tableid):
            if schemafield.mode == "REPEATED":
                columnsjsonl[schemafield.name] = [self.get_defaultvalue(schemafield)]
            else:
                columnsjsonl[schemafield.name] = self.get_defaultvalue(schemafield)

        return columnsjsonl


    def get_defaultvalue(self, schemafield):
        if schemafield.field_type in ["NUMERIC", "INTEGER"]:
            return 0
        elif schemafield.field_type in ["DATE"]:
            return "9999-12-31"
        elif schemafield.field_type in ["BOOLEAN"]:
            return "TRUE"
        elif schemafield.field_type in ["RECORD"]:
            temp_json = {}
            for f in schemafield.fields:
                temp_json[f.name] = self.get_defaultvalue(f)
            return temp_json
        else:
            return ""

from google.cloud import bigquery

class Bigquery:

    def __init__(self, project):
        self.client = bigquery.Client(project = project)


    def get_schemafields(self, full_tableid):
        return self.client.get_table(full_tableid).schema


    def get_columnslist(self, full_tableid):
        columnslist = []
        for schemafield in self.get_schemafields(full_tableid):
            if schemafield.field_type == "RECORD":
                for f in schemafield.fields:
                    columnslist.append(f"{schemafield.name}.{f.name}")
            else:
                columnslist.append(schemafield.name)

        return columnslist


    def get_columnsjsonl(self, full_tableid):
        columnsjsonl = {}
        for schemafield in self.get_schemafields(full_tableid):
            if schemafield.field_type == "RECORD":
                child_jsonl = {}
                for f in schemafield.fields:
                    child_jsonl[f.name] = self.get_defaultvalue(schemafield.field_type)
                columnsjsonl[schemafield.name] = [child_jsonl]
            else:
                columnsjsonl[schemafield.name] = self.get_defaultvalue(schemafield.field_type)

        return columnsjsonl


    def get_defaultvalue(self, fieldtype):
        if fieldtype in ["NUMERIC", "INTEGER"]:
            return 0
        elif fieldtype in ["DATE"]:
            return "9999-12-31"
        elif fieldtype in ["BOOLEAN"]:
            return "TRUE"
        else:
            return ""

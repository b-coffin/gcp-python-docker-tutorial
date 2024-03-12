def get_ifnull_sql(fieldname, fieldtype):
    if fieldtype in ["NUMERIC", "INTEGER"]:
        return f"IFNULL({fieldname}, 0)"
    elif fieldtype in ["DATE"]:
        return f"IFNULL({fieldname}, '9999-12-31')"
    elif fieldtype in ["BOOLEAN"]:
        return f"IFNULL({fieldname}, TRUE)"
    else:
        return f"IFNULL({fieldname}, '')"

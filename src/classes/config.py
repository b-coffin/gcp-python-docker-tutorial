import jmespath

class Config:

    SERVICES = {
        "BIGQUERY": "bigquery"
    }

    MODES_BQ = {
        "COMPARE": ["compare"]
    }

    def __init__(self, config_json):
        self.project = jmespath.search("project", config_json)
        self.service = jmespath.search("service", config_json)
        self.mode = jmespath.search("mode", config_json)
        self.output_format = jmespath.search("output_format", config_json) or "sql"
        if self.service == self.SERVICES["BIGQUERY"]:
            self.tables = jmespath.search("bigquery.tables", config_json)

    @property
    def service(self):
        return self.__service

    @service.setter
    def service(self, value):
        service_list = self.SERVICES.values()
        if value not in service_list:
            raise ValueError(f"\"service\" must be the followings: {', '.join(service_list)}")
        self.__service = value

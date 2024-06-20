import jmespath

class Config:

    SERVICE_BQ = "bigquery"

    def __init__(self, config_json):
        self.service = jmespath.search("service", config_json)
        self.mode = jmespath.search("mode", config_json)
        if self.service == self.SERVICES["BIGQUERY"]:
            self.tables = jmespath.search("bigquery.tables", config_json)

    @property
    def service(self):
        return self.__service

    @service.setter
    def service(self, value):
        service_list = [
            self.SERVICE_BQ
        ]
        if value not in service_list:
            raise ValueError(f"\"service\" must be the followings: {', '.join(service_list)}")
        self.__service = value

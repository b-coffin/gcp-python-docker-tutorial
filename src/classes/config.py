import jmespath

class Config:

    SERVICE_BQ = "bigquery"
    SERVICE_STORAGE = "storage"

    MODE_SELECT = "select"
    MODE_COMPARE = "compare"
    MODE_UPLOAD = "upload"
    MODE_GET = "get"

    def __init__(self, config_json):
        self.service = jmespath.search("service", config_json)
        self.mode = jmespath.search("mode", config_json)
        self.key_file = jmespath.search("key_file", config_json)
        if self.service == self.SERVICE_BQ:
            self.tables = jmespath.search("bigquery.tables", config_json)
        if self.service == self.SERVICE_STORAGE:
            self.project = jmespath.search("project", config_json)
            self.bucket = jmespath.search("bucket", config_json)
            self.blob_name = jmespath.search("blob_name", config_json)
            self.source_folder = jmespath.search("source_folder", config_json)
            self.destination_folder = jmespath.search("destination_folder", config_json)


    @property
    def service(self):
        return self.__service

    @service.setter
    def service(self, value):
        service_list = [
            self.SERVICE_BQ,
            self.SERVICE_STORAGE
        ]
        if value not in service_list:
            raise ValueError(f"\"service\"は次のいずれかでなければなりません: {', '.join(service_list)}")
        self.__service = value


    @property
    def mode(self):
        return self.__mode
    
    @mode.setter
    def mode(self, value):
        bq_mode_list = [
            self.MODE_SELECT,
            self.MODE_COMPARE
        ]
        storage_mode_list = [
            self.MODE_GET,
            self.MODE_UPLOAD
        ]
        if self.service == self.SERVICE_BQ:
            if value not in bq_mode_list:
                raise ValueError(f"\"mode\"は次のいずれかでなければなりません: {', '.join(bq_mode_list)}")
        elif self.service == self.SERVICE_STORAGE:
            if value not in storage_mode_list:
                raise ValueError(f"\"mode\"は次のいずれかでなければなりません: {', '.join(storage_mode_list)}")
        self.__mode = value


    @property
    def project(self):
        return self.__project
    
    @project.setter
    def project(self, value):
        if self.service == self.SERVICE_STORAGE:
            if value is None:
                raise ValueError(f"\"project\"は必須です")
        self.__project = value

    
    @property
    def bucket(self):
        return self.__bucket
    
    @bucket.setter
    def bucket(self, value):
        if self.service == self.SERVICE_STORAGE:
            if value is None:
                raise ValueError(f"\"bucket\"は必須です")
        self.__bucket = value


    @property
    def source_folder(self):
        return self.__source_folder
    
    @source_folder.setter
    def source_folder(self, value):
        self.__source_folder = value


    @property
    def destination_folder(self):
        return self.__destination_folder
    
    @destination_folder.setter
    def destination_folder(self, value):
        if self.service == self.SERVICE_STORAGE:
            if self.mode == self.MODE_UPLOAD:
                if value is None:
                    raise ValueError(f"\"destination_folder\"は必須です")
        self.__destination_folder = value

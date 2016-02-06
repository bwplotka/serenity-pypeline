from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError, InfluxDBClientError
from database_connector import DatabaseConnector, DatabaseConnectionException

__author__ = 'MacRomanowski'


class InfluxDbConnector(DatabaseConnector):
    DATABASE_ENGINE_NAME = 'InfluxDB'

    def __init__(self, conf):
        super(InfluxDbConnector, self).__init__(conf)

    def _get_section_name(self):
        return InfluxDbConnector.DATABASE_ENGINE_NAME

    def connect(self):
        self._dbClient = InfluxDBClient(self._hostAddress,
                                        self._hostPort,
                                        self._userName,
                                        self._userPassword,
                                        self._databaseName)

    def query_data(self, query):
        try:
            return self._dbClient.query(query)
        except InfluxDBServerError as e:
            raise DatabaseConnectionException('InfluxDB has returned 5XX HTTP error code', e)
        except InfluxDBClientError as e:
            print DatabaseConnectionException('Unable to send request to InfluxDB', e)

    def write_data(self, json_data_to_write):
        try:
            return self._dbClient.write_points(json_data_to_write)
        except InfluxDBServerError as e:
            print DatabaseConnectionException('InfluxDB has returned 5XX HTTP error code', e)
        except InfluxDBClientError as e:
            print DatabaseConnectionException('Unable to send request to InfluxDB', e)


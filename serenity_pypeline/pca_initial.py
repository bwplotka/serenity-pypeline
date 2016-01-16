from influxdb_connector import InfluxDbConnector
from filter_step import FilterStep
from sqlbuilder.smartsql import Q, compile

__author__ = 'MacRomanowski'


class PcaInitial(FilterStep):

    def __init__(self, conf):
        super(FilterStep, self).__init__()

        # TODO: type of database engine should be loaded from the workflow configuration file
        self._dbConnector = InfluxDbConnector(conf)
        self._dbConnector.connect()
        self._result = None

    def run(self, **kwargs):
        input_dict = kwargs

        """
        Just an example usage...
        """
        # TODO: Consider, how to set which metrics should be retrieved from the database
        # TODO: How big set of data we should analyze? In the meaning of time (where statement)
        query_to_execute = compile(Q().tables('"node2/load/load"').fields('value').where("time > now() - 1m"))
        database_output = self._get_data_from_database(self._format_query_to_string(query_to_execute))

        input_dict["node2/load/load"] = database_output

        self._result = input_dict
        return self._result

    def _format_query_to_string(self, query):
        return query[0] % tuple(query[1])

    def _get_data_from_database(self, query_to_execute):
        return self._dbConnector.query_data(query_to_execute)

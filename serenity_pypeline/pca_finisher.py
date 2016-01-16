import datetime
from filter_step import FilterStep
from influxdb_connector import InfluxDbConnector

__author__ = 'MacRomanowski'


class PcaFinisher(FilterStep):
    DEFAULT_CORR_PREFIX = 'corr_'
    DEFAULT_SEPARATOR = '_'
    STATUS_CODE_SUCCESSFUL = 0

    def __init__(self, conf):
        super(FilterStep, self).__init__()
        self._dbConnector = InfluxDbConnector(conf)
        """
        type of database engine should be loaded from the workflow configuration file
        """

        self._result = None

    def run(self, **kwargs):

        return PcaFinisher.STATUS_CODE_SUCCESSFUL

    def _insert_data(self):
        current_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
        print 'Inserting data...'

    def _create_record_json(self, first_param_name, second_param_name, time, value):
        measurement_name = PcaFinisher.DEFAULT_CORR_PREFIX + first_param_name \
                           + PcaFinisher.DEFAULT_SEPARATOR + second_param_name

        record_json = {
            "measurement": measurement_name,
            "tags": {
                "host": "localhost"  # consider changing it to more proper data
            },
            "time": time,
            "field": {
                "values": value
            }
        }

        return record_json


import datetime

from serenity_pypeline.logger import log
from serenity_pypeline.db.influxdb_connector import InfluxDbConnector
from serenity_pypeline.filters.filter import Filter


class PcaFinisherException(Exception):
    pass


class PcaFinisher(Filter):
    KEY_FOR_DATA = 'data_to_insert'
    DEFAULT_CORR_PREFIX = 'corr_'
    DEFAULT_SEPARATOR = '_'
    STATUS_CODE_SUCCESSFUL = 0

    def __init__(self, conf):
        super(PcaFinisher, self).__init__(conf)

        # TODO: type of database engine should be loaded from the workflow configuration file
        self._dbConnector = InfluxDbConnector(conf)
        self._dbConnector.connect()

        self._result = None

    def run(self, **kwargs):
        if kwargs.has_key(PcaFinisher.KEY_FOR_DATA):
            self._insert_data(kwargs[PcaFinisher.KEY_FOR_DATA])
            return PcaFinisher.STATUS_CODE_SUCCESSFUL
        else:
            raise PcaFinisherException('No data for insert retrieved from a previous step. Failing...')

    def _insert_data(self, data_to_insert):
        current_time = datetime.datetime.now()
        current_epoch = int(current_time.strftime("%s")) * 1000
        current_time_iso = current_time.isoformat()

        for first_param_name, second_param_name, value in data_to_insert:
            json_record = \
                self._create_record_json(first_param_name, second_param_name, value, current_time_iso, current_epoch)
            self._dbConnector.write_data(json_record)  # shouldn't it be inserted to a different database?

    def _create_record_json(self, first_param_name, second_param_name, value, time, epoch):
        measurement_name = PcaFinisher.DEFAULT_CORR_PREFIX + first_param_name \
                           + PcaFinisher.DEFAULT_SEPARATOR + second_param_name

        record_json = {
            "measurement": measurement_name,
            "tags": {
                "calculate_date": time  # consider some more specific tags for correlation
            },
            "time": epoch,
            "field": {
                "values": value
            }
        }

        return record_json


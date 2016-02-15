import datetime, math

from serenity_pypeline.logger import log
from serenity_pypeline.db.influxdb_connector import InfluxDbConnector
from serenity_pypeline.filters.filter import Filter


class CfFinisherException(Exception):
    pass


class CfFinisher(Filter):
    KEY_FOR_DATA = 'data_to_insert'
    KEY_NAME = 'measurement'
    DEFAULT_CORR_PREFIX = 'cf_corr_'
    DEFAULT_SEPARATOR = '_'
    STATUS_CODE_SUCCESSFUL = 0

    def __init__(self, conf):
        super(CfFinisher, self).__init__(conf)

        # TODO: type of database engine
        # should be loaded from the workflow configuration file
        self._dbConnector = InfluxDbConnector(conf)
        self._dbConnector.connect()

        self._result = None
        self.node = conf['default']['node']

    def run(self, **kwargs):
        # TODO: 2 matrixes as result?
        log.debug("CF_FINISH")
        if CfFinisher.KEY_FOR_DATA in kwargs:
            self._insert_data(kwargs[CfFinisher.KEY_FOR_DATA],
                    kwargs[CfFinisher.KEY_NAME])
            return CfFinisher.STATUS_CODE_SUCCESSFUL
        else:
            raise CfFinisherException(
                'No data for insert retrieved from a previous step. Failing...')

    def _insert_data(self, data_to_insert, measurement):
        result = []
        for name, data in data_to_insert.iteritems():
            json_record = self._create_record_json(measurement+'_'+name,
                    data)
            result.append(json_record)

        self._dbConnector.write_data(result)

    def _create_record_json(self, measurement, data):

        record_json = {
            "measurement": measurement + '_' + self.node,
            "fields": {
            }
        }

        for i, val in enumerate(data):
            record_json['fields']['f'+str(i+1)] = val
        return record_json


import ConfigParser
from abc import ABCMeta
from ConfigParser import NoSectionError, NoOptionError
from abc import abstractmethod

__author__ = 'MacRomanowski'


class DatabaseConnector(object):
    __metaclass__ = ABCMeta

    DATABASES_SECTION_PARAM = "databases_connector"
    DATABASES_CONFIGURATION_PATH_PARAM = "configuration_path"

    @abstractmethod
    def __init__(self, conf):
        self._config_file_path \
            = conf[DatabaseConnector.DATABASES_SECTION_PARAM][DatabaseConnector.DATABASES_CONFIGURATION_PATH_PARAM]

        # Init of further used variables
        self._configuration_loaded = False
        self._config_parser = None
        self._dbClient = None

        # should read this from config file
        self._hostAddress = None
        self._hostPort = None
        self._userName = None
        self._userPassword = None
        self._databaseName = None

        self._read_configuration()

    @abstractmethod
    def _get_section_name(self):
        raise NotImplementedError('user must define _get_section_name method')

    def _get_configuration_parser(self):
        if self._config_parser is None:
            self._config_parser = ConfigParser.ConfigParser()
            self._config_parser.read(self._config_file_path)

        return self._config_parser

    def _read_configuration_option(self, option):
        try:
            return self._get_configuration_parser().get(self._get_section_name(), option)
        except (NoSectionError, NoOptionError) as e:
            print 'Configuration file is incomplete.', e

    def _read_configuration(self):
        self._hostAddress = self._read_configuration_option('HostAddress')
        self._hostPort = self._read_configuration_option('HostPort')
        self._userName = self._read_configuration_option('UserName')
        self._userPassword = self._read_configuration_option('UserPassword')
        self._databaseName = self._read_configuration_option('DatabaseName')

    def is_ready_to_connect(self):
        return self._configuration_loaded

    @abstractmethod
    def connect(self):
        """
        Connects to given database
        """
        raise NotImplementedError('user must define connect method')

    @abstractmethod
    def query_data(self, query):
        """
        Execute query on database
        :param query: Query to be executed on database
        :type query: str
        :return: response from database
        """
        raise NotImplementedError('user must define query_data method')

    @abstractmethod
    def write_data(self, json_data_to_write):
        """
        Write data (given as JSON) to the database
        :param json_data_to_write: data to write
        """
        raise NotImplementedError('user must define write_data method')


class DatabaseConnectionException(Exception):
    pass
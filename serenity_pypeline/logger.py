import logging

LOGGING_LVL = logging.DEBUG

log = logging.getLogger(__name__)
log.setLevel(LOGGING_LVL)

ch = logging.StreamHandler()
ch.setLevel(LOGGING_LVL)
log.addHandler(ch)
#!/usr/bin/env python
#
# place this under /usr/share/weewx/user
#
import weewx
from weewx.engine import StdService
import logging
import logging.handlers
import requests
import time

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
loggerName = next(iter(str(__file__).split('.')), None)
rotate_file_handler = logging.handlers.RotatingFileHandler(
    loggerName + '.log', maxBytes=1000000, backupCount=1)
logger.addHandler(rotate_file_handler)
formatter = logging.Formatter(
    '%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
rotate_file_handler.setFormatter(formatter)


class InfluxWriter(StdService):

    class IntervalGroup(object):

        def __init__(self, interval, key_list):
            self.__key_list = key_list
            self.__interval = interval
            self.__lastupdate = 0
            pass

        def needs_update(self):
            if time.time() > self.__lastupdate + self.__interval:
                return True
            else:
                return False

        def update(self):
            self.__lastupdate = time.time()

        def containskey(self, key):
            return key in self.__key_list

        @property
        def key_list(self):
            return self.__key_list

    def __init__(self, engine, config_dict):
        super(InfluxWriter, self).__init__(engine, config_dict)
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        # self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        self.sess = requests.Session()
        self.interval_groups = [
            self.IntervalGroup(
                2.5, ['windDir', 'windGust', 'windGustDir', 'windSpeed']),
            self.IntervalGroup(
                10, ['dewpoint', 'heatindex', 'outTemp', 'windchill']),
            self.IntervalGroup(
                20, ['rain', 'rainRate', 'dayRain', 'stormRain']),
            self.IntervalGroup(
                50, ['outHumidity']),
            self.IntervalGroup(
                60, ['barometer', 'inTemp', 'inHumidity', 'pressure', 'inDewpoint']),
            self.IntervalGroup(
                600, ['windSpeed10', 'consBatteryVoltage', 'txBatteryStatus']),
            self.IntervalGroup(
                3600, ['forecastIcon', 'forecastRule'])
        ]

    def new_loop_packet(self, event):
        def in_list(s, lst):
            for ignore in lst:
                if ignore in s:
                    return True
            return False
        packet = event.packet

        timestamp = packet.get('dateTime', None)
        if not timestamp:
            logging.error('No timestamp in packet: {}'.format(packet))
            return
        else:
            # influxdb expects timestamps in nanoseconds
            timestamp *= 1e9

        key_list = []
        for group in self.interval_groups:
            if group.needs_update():
                group.update()
                key_list.extend(group.key_list)

        points = []
        for i, key in enumerate(key_list):
            value = packet.get(key, None)
            if value != 'null' and value is not None:
                points.append(
                    '{0},host=VantageVue value={1} {2:.0f}'.format(key, value, timestamp))

        data = '\n'.join(points)

        if len(points) > 0:
            logging.debug('logging points {}'.format(points))
            rsp = self.sess.post('http://192.168.0.110:8086/write?db=weewx', data)
            if rsp.status_code < 200 or rsp.status_code > 299:
                logging.debug(
                    'InfluxError: {}: {}'.format(rsp.status_code, rsp.text))

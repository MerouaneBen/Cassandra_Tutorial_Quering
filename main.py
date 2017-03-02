#!/usr/bin/env python

# -*- coding: utf-8 -*-
"""
    Simple Query Cassandra ..
    :author: `Merouane Benthameur. <merouane.benth@gmail.com>`_.
"""
from __future__ import absolute_import
import logging
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


__version__ = '0.1.0'

#: Log message format when logging to file.
LOG_FORMAT = '-> %(asctime)s || (%(module)s)%(pathname)s:%(lineno)d\n[%(levelname)s] %(message)s\n'
logging.basicConfig(filename='log_file.log', format=LOG_FORMAT, level=logging.DEBUG)

auth_provider = PlainTextAuthProvider(
    username='Username', password='Password')


def get_data(date_string, entity_type):
    """
    this function query Cassandra and return the data
    :param date_string: str date formatted yyyymmdd to filter the query
    :param entity_type: str parameter to filter the query
    :return: result set from Cassandra
    """
    # connect to database server
    # prod  Environment
    cluster = Cluster(['Host_Cassandra'],   # put the server host of Cassandra
                      auth_provider=auth_provider)
    session = cluster.connect('eventlog')
    session.default_timeout = 30  # this is in *seconds* (this timeout is necessary if you're querying a huge dataset)

    last_day = "'{}'".format(date_string)
    entity_type = "'{}'".format(entity_type)

    # query Cassandra (Looks like a SQL queries...)
    try:
        logging.info('Executing query in cassandra ...')
        rows = session.execute(""" select id,
                                         entity_type,
                                         event_type,
                                         created_at,
                                         data
                                  from event_entity_by_day
                                  where event_day = """ + last_day + """
                                  and entity_type = """ + entity_type)

        logging.info('Query has been executed in cassandra ...')

        return rows
    except Exception as e:
        logging.warning(e.args)


if __name__ == '__main__':

    # entry point to our programme
    try:
        for row in get_data('20120215', 'Some_Parameter'):
            # for example we're interested to  get only the data element...
            print(row.data)

    except Exception as e:
        print(str(e))

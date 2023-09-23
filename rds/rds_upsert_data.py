# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import mysql.connector
from awsglue.utils import getResolvedOptions


params = [
    'db_host',
    'db_port',
    'db_user',
    'db_password',
    'db_database',
    'ticket_id_to_be_updated'
]
args = getResolvedOptions(sys.argv, params)

cnx = mysql.connector.connect(
    host=args['db_host'],
    port=args['db_port'],
    user=args['db_user'],
    password=args['db_password'],
    database=args['db_database']
)
cur = cnx.cursor()


def update_data(cursor, connection):
    ticket_id = args['ticket_id_to_be_updated']

    print("Selecting one record from table {}".format("customer"))
    cursor.execute("SELECT customer_id FROM customer ORDER BY RAND() LIMIT 1")
    rows = cursor.fetchall()
    customer_id = ""
    for row in rows:
        customer_id = row[0]

    update_event = ("UPDATE ticket_activity SET purchased_by={}, updated_at=now() WHERE ticket_id={}".format(customer_id, ticket_id))
    cursor.execute(update_event)
    connection.commit()


def read_data(cursor):
    cursor.execute("SELECT * FROM ticket_activity")
    rows = cursor.fetchall()
    for row in rows:
        print(row)


read_data(cur)
update_data(cur, cnx)
read_data(cur)

cur.close()
cnx.close()

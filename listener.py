from database import get_connection
import select
import psycopg2
import psycopg2.extensions

import os 
script_directory = os.path.dirname(os.path.abspath(__file__))
import sys 
sys.path.append(script_directory)

channel = 'test'

def listen_channel(channel, function):
    with get_connection() as con:
        
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        curs = con.cursor()
        curs.execute(f"LISTEN {channel};")
        
        print(f"Waiting for notifications on channel {channel}")
        while True:
            if select.select([con],[],[],5) == ([],[],[]):
                print("no new data")
            else:
                con.poll()
                while con.notifies:
                    notify = con.notifies.pop(0)
                    function()
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)

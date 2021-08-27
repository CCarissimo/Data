from database import get_connection
import select
import psycopg2
import psycopg2.extensions

channel = 'test'

def listen_channel(channel):
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
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)

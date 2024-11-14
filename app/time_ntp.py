import ntplib
from datetime import datetime
import logger

time_offset = None

def sync_chile_time():
    global time_offset
    if time_offset is not None: return
    
    try:
        client = ntplib.NTPClient()
        response = client.request('ntp.shoa.cl', version=3)
        
        ntp_chile_time = datetime.fromtimestamp(response.tx_time)
        system_time = datetime.now()
        
        time_offset = ntp_chile_time - system_time
    except Exception as e:
        logger.write(f"Error al consultar servidor NTP Chile: {str(e)}")


def now():
    global time_offset
    sync_chile_time()
    if time_offset is None:
        return datetime.now()
    return datetime.now() + time_offset

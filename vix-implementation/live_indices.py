import pandas as pd

from datetime import datetime
from vxbt_calc import vxbt_calc as vc
from apscheduler.schedulers.blocking import BlockingScheduler

def write_indices():
    timestamp = datetime.now().replace(second=0, microsecond=0)
    vxbt, gvxbt, avxbt = vc.get_indices()
    df = pd.DataFrame({'timestamp': timestamp, 'vxbt': vxbt, 'gvxbt': gvxbt, 'avxbt': avxbt}, index=[0])
    df.to_csv('live_indices.csv', mode='a', header=False)

scheduler = BlockingScheduler()
scheduler.add_job(write_indices, 'cron', minute='*')
scheduler.start()
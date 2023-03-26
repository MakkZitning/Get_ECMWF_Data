#  原出处：https://github.com/jiangleads/Get_ECMWF_Data/commit/2e8e114fc3637035da0a2071f73593ff288b6ee5

from queue import Queue
from threading import Thread
import cdsapi
from time import time
import datetime
import os


def cds_request(filename, riqi):
    c = cdsapi.Client()
    c.retrieve(
        'reanalysis-era5-pressure-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable':
                [
                    'divergence', 'geopotential', 'potential_vorticity',
                    'relative_humidity', 'specific_humidity', 'temperature',
                    'u_component_of_wind', 'v_component_of_wind', 'vertical_velocity', 'vorticity',
                ],
            'time':
                [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00',
                ],
            'pressure_level':
                [
                    '700',
                ],
            'area': [70, 40, 0, 160, ],
            'year': riqi[0:4],
            'month': riqi[-4:-2],
            'day': riqi[-2:],
        },
        filename)


def downloadonefile(riqi):
    ts = time()
    filename = 'D:\\meteo_data\\ERA5\\700\\' + riqi + '.nc'
    if os.path.isfile(filename):
        print(' ok', filename)
    else:
        print(filename)
        cds_request(filename, riqi)


# 下载脚本
class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # 从队列中获取任务并扩展tuple
            riqi = self.queue.get()
            downloadonefile(riqi)
            self.queue.task_done()


# 主程序
def main():
    # 程序起始时间
    ts = time()
    # 下载日期跨度
    begin = datetime.date(2020, 5, 1)
    end = datetime.date(2020, 5, 1)
    d = begin
    delta = datetime.timedelta(days=1)
    # 建立下载日期序列
    links = []
    while d <= end:
        riqi = d.strftime('%Y%m%d')
        mon = riqi[-4:-2]
        # 只选取5到10月
        monlist = ['04', '05', '06', '07', '08', '09'] #'05', '06', '07', '08', '09', '10'
        # monlist = ['04'] #'05', '06', '07', '08', '09', '10'
        if any(m in mon for m in monlist):
            links.append(str(riqi))
            d += delta
        else:
            d += delta
            continue
    # 创建一个主进程与工作进程通信
    queue = Queue()
    # 20191119更新# 新的请求规则 https://cds.climate.copernicus.eu/live/limits
    # 注意，每个用户同时最多接受4个request https://cds.climate.copernicus.eu/vision
    # 创建四个工作线程
    for x in range(4):
        worker = DownloadWorker(queue)
        # 将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
        worker.daemon = True
        worker.start()
    # 将任务以tuple的形式放入队列中
    for link in links:
        queue.put(link)
    # 让主线程等待队列完成所有的任务
    queue.join()
    print('Took {}'.format(time() - ts))


if __name__ == '__main__':
    main()

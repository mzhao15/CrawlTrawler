
from datetime import datetime, timedelta, time


day1 = datetime(2016, 1, 1)
# print(day1)
print('today is {}'.format(day1.date()))
date = day1.date()
print(date)
date2 = date + timedelta(days=1)
print(date2)

day = '2016-01-01'
dday = datetime.strptime(day, '%Y-%m-%d')
dday = dday.date()
# print(dday)
tday = datetime.today().date()
day2 = tday + timedelta(days=2)
day3 = tday - timedelta(days=2)
# print(day3)

datelist = [tday, day2, day3, dday]
newdatelist = sorted(datelist)

time0 = datetime.combine(datetime.strptime('2016-01-01', '%Y-%m-%d'), time(0, 0, 0))
# print(type(time0))
time1 = time0 - timedelta(days=30)
# print(time1)

time2 = time0 + timedelta(days=1)
# print(time2)

if time1 > time0:
    print('yes')

sometime = '2016-01-01 00:00:00'
realsometime = datetime.strptime(sometime, '%Y-%m-%d %H:%M:%S')
# print(type(realsometime))
strtime = realsometime.strftime("%Y-%m-%d %H:%M:%S")
# print(type(strtime))

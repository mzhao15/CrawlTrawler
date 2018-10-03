from datetime import datetime, timedelta

start_date = '2016-01-01'
end_date = '2016-01-30'

start_date_date = datetime.strptime(start_date, "%Y-%m-%d")
print(type(start_date_date))
start_date_date += timedelta(days=1)
print(start_date_date.date())

print(str(start_date_date.date()))

if start_date < end_date:
    print('true')

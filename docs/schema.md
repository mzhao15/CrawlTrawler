Cassandra table schemas

1. table_robot_ip(
    ip: varchar(80),
    detected_num: int,
    detected_date: yy-mm-dd,
    flag: int
    primary key(ip, detected_date, flag)
)

    flag:
        0: download from too many companies,
        1: download too many times in a minute
        2: download too many times in a day


2. table_download_times(
    id: serial primary key,
    visit_date: yy-mm-dd,
    cik: varchar(80),
    num_of_visits: int
)

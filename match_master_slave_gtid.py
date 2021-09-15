

#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import logging


def connect_mysql_instance(host, port, user, pwd, db, charset):
    try:
        db = pymysql.connect(
            host=host, port=port,
            user=user, password=pwd,
            db=db, charset=charset)
        return db, True
    except BaseException as e:
        return None, False


# we assume the master & slave share the same user/pwd
def judge_gtid_between_master_slave(host, port, user, pwd):
    logging.basicConfig(filename='gtid.log', level=logging.INFO)

    dbSlave, ok = connect_mysql_instance(
        host, port, user, pwd, 'mysql', 'utf8')
    if not ok:
        logging.error("cant connect to instance(%s:%d)" %
                      (host, port))
        return
    cursor = dbSlave.cursor()

    try:
        resultLen = cursor.execute("show slave status")  # 尝试获取主从信息
    except BaseException as e:
        logging.error("cant query slave status on instance(%s:%d)" %
                      (host, port))
        dbSlave.close()
        return

    if resultLen == 0:  # 不是从库
        logging.info("instance(%s:%d) is not a slave" %
                     (host, port))
        dbSlave.close()
        return

    # 检查结果，一定只有一行记录
    results = cursor.fetchall()
    for result in results:
        master_host, master_port = result[1], result[3]
        slave_io_running, slave_sql_running = result[10], result[11]
        last_io_error_timestamp, last_sql_error_timestamp = result[-11], result[-10]

        # 如果主从异常运行，则记录下对应的error时间，退出
        if slave_io_running == "No" or slave_sql_running == "No":
            logging.warning("io/sql not running between slave(%s:%d) master(%s:%d) io(%s-%d), sql(%s-%d)" %
                            {host, port, master_host, master_port, slave_io_running, last_io_error_timestamp, slave_sql_running, last_sql_error_timestamp})
            continue

        # 正常运行，则记录GTID
        gtid_set_slave = []
        try:
            resultLen = cursor.execute(
                "select * from gtid_executed")
        except BaseException as e:
            print(e)
            dbSlave.close()
            continue

        if resultLen > 0:
            for result in cursor.fetchall():
                uuid, start, end = result[0], result[1], result[2]
                gtid = uuid
                if start != None:
                    gtid += ":" + str(start)
                if end != None:
                    gtid += "-" + str(end)
                gtid_set_slave.append(gtid)
        dbSlave.close()

        # 进入对应主库
        dbMaster, ok = connect_mysql_instance(
            master_host, master_port, user, pwd, 'mysql', 'utf8')
        if not ok:
            logging.error("cant connect to master(%s:%d) for slave(%s:%d)" %
                          (host, port, master_host, master_port))
            continue
        cursor = dbMaster.cursor()

        try:
            resultLen = cursor.execute("select * from gtid_executed")
        except BaseException as e:
            logging.error("cant query GITD to master(%s:%d) for slave(%s:%d)" %
                          (host, port, master_host, master_port))
            continue
        results = cursor.fetchall()
        gtid_set_master = []
        if len(results) > 0:
            for result in results:
                uuid, start, end = result[0], result[1], result[2]
                gtid = uuid
                if start != None:
                    gtid += ":" + str(start)
                if end != None:
                    gtid += "-" + str(end)
                gtid_set_master.append(gtid)

        # 比较主从
        gtid_set_master_str = ",".join(gtid_set_master)
        gtid_set_slave_str = ",".join(gtid_set_slave)

        try:
            cursor.execute("select GTID_SUBSET('%s', '%s')" %
                           (gtid_set_slave_str, gtid_set_master_str))
        except BaseException as e:
            logging.error("cant query GITD_SUBSET between master(%s:%d) slave(%s:%d)" %
                          (host, port, master_host, master_port))
            continue
        results = cursor.fetchall()

        if results[0][0] == 1:
            pass  # 没有问题，下一个
            logging.info("GTID matched(%s:%d) master(%s-%d)" %
                         (host, port, master_host, master_port))
        else:
            pass  # 有问题，记录下信息
            try:
                cursor.execute("select GTID_SUBTRACT('%s', '%s')" %
                               (gtid_set_slave_str, gtid_set_master_str))
            except BaseException as e:
                logging.error("cant query GITD_SUBTACT between master(%s:%d) slave(%s:%d)" %
                              (host, port, master_host, master_port))
                continue
            results = cursor.fetchall()
            gtid_set_subtact = []
            for result in results:
                gtid_set_subtact.append(result[0])
            logging.warning("GTID not matched between slave(%s:%d) master(%s-%d) subtract(%s)" %
                            (host, port, master_host, master_port, ",".join(gtid_set_subtact)))

        dbMaster.close()


if __name__ == '__main__':
    judge_gtid_between_master_slave("192.168.1.1", 3306, 'root', '')

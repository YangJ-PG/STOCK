# -*- coding: utf-8 -*-
"""
    同花顺概念数据
    User: 杨健
    Date: 2025 / 02 / 10
    Time: 09：40
"""
import datetime
import logging
import os
from logging.handlers import RotatingFileHandler

import pymysql
import configparser
import json
import config

# 数据结构定义
def createEmptyObj(result, *args):
    if args[0] not in result:
        result[args[0]] = {}
    if len(args) >= 2:
        if args[1] not in result[args[0]]:
            result[args[0]][args[1]] = {}
    if len(args) >= 3:
        if args[2] not in result[args[0]][args[1]]:
            result[args[0]][args[1]][args[2]] = {}
    if len(args) >= 4:
        if args[3] not in result[args[0]][args[1]][args[2]]:
            result[args[0]][args[1]][args[2]][args[3]] = {}
    if len(args) >= 5:
        if args[4] not in result[args[0]][args[1]][args[2]][args[3]]:
            result[args[0]][args[1]][args[2]][args[3]][args[4]] = {}
    return result


# 股票池数据解析
def getStockPool(poolData):
    arr = []
    if poolData:
        poolData = poolData.split(',')
        for val in poolData:
            if val:
                if ":" in val:
                    arr.append(val.split(":")[1])
                else:
                    arr.append(val)
    return arr


# 获取当前时间
def getCurTime():
    cur_time = datetime.datetime.now()
    return str(cur_time.strftime('%Y-%m-%d %H:%M:%S'))

# 股票加市场
def stockToStk(stockId):
    stockId = int(stockId)
    stockIdStr = str(1000000+stockId)[1:]
    if stockId < 600000:
        stkId = '2' + stockIdStr
    else:
        stkId = '1' + stockIdStr if stockId >= 600000 and stockId < 700000 else '3' + stockIdStr
    return stkId


class grap:
    def __init__(self):
        logger.info('========= grap begin {} ========='.format(getCurTime()))

        # 数据库连接
        cnx = pymysql.connect(host=config.host,
                              user=config.user,
                              port=config.port,
                              password=config.password,
                              database=config.database)
        cnx.begin()
        cursor = cnx.cursor()

        try:

            try:
                if os.path.exists(config.conceptRelation_url) and os.path.getsize(config.conceptRelation_url) > 0:
                    conceptRelation = None
                    with open(config.conceptRelation_url, 'r', encoding='utf8') as f:
                        conceptRelation = json.load(f)

                    conceptData = []
                    if conceptRelation is not None:
                        if conceptRelation['status_msg'] == 'success' and len(conceptRelation['data']) > 0:
                            for items in conceptRelation['data']:
                                if 'children_concepts' in items and len(items['children_concepts']) > 0:
                                    for iitems in items['children_concepts']:
                                        conceptData.append((
                                            items['index_code'] if len(items['index_code']) > 0 else "None",
                                            'BK' + items['index_code'][2:] if len(items['index_code']) > 0 else "None",
                                            items['concept_name'],
                                            items['market_id'],
                                            items['degree'],
                                            items['concept_degree'],
                                            iitems['index_code'] if len(iitems['index_code']) > 0 else "None",
                                            'BK' + iitems['index_code'][2:] if len(
                                                iitems['index_code']) > 0 else "None",
                                            iitems['concept_name'],
                                            iitems['market_id'],
                                            iitems['degree'],
                                            iitems['concept_degree'],
                                            getCurTime()
                                        ))

                        # 概念关联度
                        if len(conceptData) > 0:
                            conceptStr = str(','.join(str(i) for i in conceptData))
                            cursor.execute("DELETE FROM `bk_concept`")
                            sql = "INSERT INTO `bk_concept` (`p_stock_id`, `p_stock_code`, `p_stock_name`, `p_market_id`, `p_degree`, `p_concept_degree`," \
                                  " `stock_id`, `stock_code`, `stock_name`, `market_id`, `degree`, `concept_degree`, `date_create`) VALUES " + conceptStr
                            cursor.execute(sql)
                            cnx.commit()
                        logger.info('========= 1.概念关联度:数据更新完毕 {} ========='.format(getCurTime()))
                else:
                    logger.warning('========= 1.概念关联度:文件异常 {} ========='.format(getCurTime()))
            except Exception as e:
                cnx.rollback()
                logger.debug('========= 1.概念关联度:程序异常({}) {} ========='.format(str(e), getCurTime()))

            del conceptRelation, conceptData

            try:
                if os.path.exists(config.stockBlock_url) and os.path.getsize(config.stockBlock_url) > 0 and os.path.exists(
                        config.stockLink_url) and os.path.getsize(config.stockLink_url) > 0:
                    # 概念&概念下股票列表
                    stockBlock = configparser.RawConfigParser(strict=False, interpolation=None)
                    stockBlock.optionxform = lambda option: option
                    stockBlock.read(config.stockBlock_url, encoding='GBK')

                    # 概念代码
                    stockLink = configparser.RawConfigParser(strict=False, interpolation=None)
                    stockLink.optionxform = lambda option: option
                    stockLink.read(config.stockLink_url, encoding='GBK')

                    # 版本号
                    configVer = dict(stockBlock.items('ConfigInfo'))['ConfigVer']

                    # 所有概念
                    BLOCK_TREE_ROOT = stockBlock.items('BLOCK_TREE_ROOT')
                    code = None
                    for key, code in BLOCK_TREE_ROOT:
                        code = code
                    treeCodes = stockBlock.items(code)

                    # 概念名称
                    gnStockNames = dict(stockBlock.items('BLOCK_NAME_MAP_TABLE'))

                    # 目标概念
                    codeTargets = []
                    types = {'概念': 1, '行业': 2, '地域': 3}
                    codeTypes = {}
                    for code in gnStockNames:
                        if gnStockNames[code] in types:
                            codeTargets.append(code)
                            codeTypes[code] = types[gnStockNames[code]]

                    # 概念代码
                    gnStockIds = {}
                    for key in stockLink:
                        items = stockLink.items(key)
                        for gnStockId, CODE in items:
                            if len(gnStockId) == 6 and gnStockId[0] == '8':
                                gnStockIds[CODE.split(':')[1]] = gnStockId

                    # 股票池
                    stockPool = dict(stockBlock.items('BLOCK_STOCK_CONTEXT'))

                    del BLOCK_TREE_ROOT, stockLink

                    # 检查版本
                    cursor.execute("SELECT `configVer` FROM `bk_version`")
                    results = cursor.fetchall()
                    cursor.execute("UPDATE `bk_version` SET `date_exec` = %s", (getCurTime()))
                    if results[0][0] != configVer:
                        cursor.execute("UPDATE `bk_version` SET `configVer` = %s, `date_update` = %s, `date_exec` = %s",
                                       (configVer, getCurTime(), getCurTime()))

                        result = {}
                        for CODE, ID in treeCodes:
                            # 概念第一级
                            CODE = str(CODE)
                            if CODE in codeTargets:
                                result = createEmptyObj(result, CODE)
                                result[CODE] = {
                                    'identify': ID,
                                    'gnStockId': gnStockIds.get(CODE, ''),
                                    'gnStockName': gnStockNames.get(CODE, '')
                                }
                                # 概念第二级
                                if ID[0] == '@':
                                    childCodes = stockBlock.items(ID)
                                    for CODE2, ID2 in childCodes:
                                        CODE2 = str(CODE2)
                                        result = createEmptyObj(result, CODE, 'child', CODE2)
                                        result[CODE]['child'][CODE2] = {
                                            'identify': ID2,
                                            'gnStockId': gnStockIds.get(CODE2, ''),
                                            'gnStockName': gnStockNames.get(CODE2, '')
                                        }
                                        # 概念第三级
                                        if ID2[0] == '@':
                                            child2Codes = stockBlock.items(ID2)
                                            for CODE3, ID3 in child2Codes:
                                                CODE3 = str(CODE3)
                                                result = createEmptyObj(result, CODE, 'child', CODE2, 'child', CODE3)
                                                result[CODE]['child'][CODE2]['child'][CODE3] = {
                                                    'identify': ID3,
                                                    'gnStockId': gnStockIds.get(CODE3, ''),
                                                    'gnStockName': gnStockNames.get(CODE3, '')
                                                }
                                                # 概念第四级(暂无)
                                                if ID3[0] == '@':
                                                    pass
                                                else:
                                                    result[CODE]['child'][CODE2]['child'][CODE3][
                                                        'stockpool'] = getStockPool(stockPool.get(CODE3, ''))
                                        else:
                                            result[CODE]['child'][CODE2]['stockpool'] = getStockPool(
                                                stockPool.get(CODE2, ''))
                                else:
                                    result[CODE]['stockpool'] = getStockPool(stockPool.get(CODE, ''))

                        del treeCodes, gnStockNames, gnStockIds, stockPool, stockBlock

                        # 概念股票池-股票进出数据
                        inoutArr = []
                        cursor.execute("select  main.`stock_id`,main.`parentcode` FROM `bk_stock_inout` main "
                                       "inner join "
                                       "(select `stock_id`,`parentcode`,max(`date_create`) date_create from `bk_stock_inout` group by stock_id, `parentcode` ) temp "
                                       "on main.`stock_id` = temp.`stock_id` and main.`parentcode` = temp.`parentcode` and main.`date_create` = temp.`date_create` and main.`state` = 1")
                        results = cursor.fetchall()
                        inout_old_arr = {}
                        for stock_id, parentcode in results:
                            if parentcode not in inout_old_arr:
                                inout_old_arr[parentcode] = []
                            inout_old_arr[parentcode].append(int(stock_id))

                        del results

                        # 数据组装
                        bkArr = []
                        stockArr = []
                        for code in result:
                            # 概念第一级
                            identify = result[code]['identify']
                            gnStockId = result[code]['gnStockId'] if len(result[code]['gnStockId']) > 0 else "None"
                            gnStockCode = 'BK' + result[code]['gnStockId'][2:] if len(
                                result[code]['gnStockId']) > 0 else "None"
                            gnStockName = result[code]['gnStockName']
                            codeType = codeTypes[code]
                            # 概念
                            bkArr.append(
                                (gnStockId, gnStockCode, gnStockName, identify, code, 'None', codeType, getCurTime(), 1, getCurTime()))
                            # 股票池
                            if 'stockpool' in result[code] and len(result[code]['stockpool']) > 0:
                                instate = False
                                if code in inout_old_arr:
                                    instate = True

                                for stockId in result[code]['stockpool']:
                                    stockId = str(1000000 + int(stockId))[1:]
                                    stkId = stockToStk(stockId)
                                    stockArr.append((stockId, stkId, code, getCurTime()))
                                    if not instate:
                                        inoutArr.append((stockId, stkId, code, 1, getCurTime()))

                                if instate:
                                    # 在inout_old_arr中但不在stockpool中标的:剔除标的
                                    diff1 = list(set(inout_old_arr[code]) - set([int(x) for x in result[code]['stockpool']]))
                                    for stockId in diff1:
                                        inoutArr.append((str(1000000 + int(stockId))[1:], stockToStk(stockId), code, 0, getCurTime()))

                                    # 在stockpool中但不在inout_old_arr中标的:新加入标的
                                    diff2 = list(set([int(x) for x in result[code]['stockpool']]) - set(inout_old_arr[code]))
                                    for stockId in diff2:
                                        inoutArr.append((str(1000000 + int(stockId))[1:], stockToStk(stockId), code, 1, getCurTime()))

                                    del inout_old_arr[code]


                            # 概念第二级
                            if 'child' in result[code] and len(result[code]['child']) > 0:
                                child = result[code]['child']
                                for code2 in child:
                                    identify2 = child[code2]['identify']
                                    gnStockId2 = child[code2]['gnStockId'] if len(
                                        child[code2]['gnStockId']) > 0 else "None"
                                    gnStockCode2 = 'BK' + child[code2]['gnStockId'][2:] if len(
                                        child[code2]['gnStockId']) > 0 else "None"
                                    gnStockName2 = child[code2]['gnStockName']
                                    # 概念
                                    bkArr.append((gnStockId2, gnStockCode2, gnStockName2, identify2, code2, code, codeType,
                                                  getCurTime(), 1, getCurTime()))
                                    # 股票池
                                    if 'stockpool' in child[code2] and len(child[code2]['stockpool']) > 0:
                                        instate2 = False
                                        if code2 in inout_old_arr:
                                            instate2 = True

                                        for stockId2 in child[code2]['stockpool']:
                                            stockId2 = str(1000000 + int(stockId2))[1:]
                                            stkId2 = stockToStk(stockId2)
                                            stockArr.append((stockId2, stkId2, code2, getCurTime()))
                                            if not instate2:
                                                inoutArr.append((stockId2, stkId2, code2, 1, getCurTime()))

                                        if instate2:
                                            # 在inout_old_arr中但不在stockpool中的标的:剔除标的
                                            diff1 = list(set(inout_old_arr[code2]) - set([int(x) for x in child[code2]['stockpool']]))
                                            for stockId in diff1:
                                                inoutArr.append((str(1000000 + int(stockId))[1:], stockToStk(stockId), code2, 0, getCurTime()))

                                            # 在stockpool中但不在inout_old_arr中标的:新加入标的
                                            diff2 = list(set([int(x) for x in child[code2]['stockpool']]) - set(inout_old_arr[code2]))
                                            for stockId in diff2:
                                                inoutArr.append((str(1000000 + int(stockId))[1:], stockToStk(stockId), code2, 1, getCurTime()))

                                            del inout_old_arr[code2]

                                    # 概念第三级
                                    if 'child' in child[code2] and len(child[code2]['child']) > 0:
                                        child2 = child[code2]['child']
                                        for code3 in child2:
                                            identify3 = child2[code3]['identify']
                                            gnStockId3 = child2[code3]['gnStockId'] if len(
                                                child2[code3]['gnStockId']) > 0 else "None"
                                            gnStockCode3 = 'BK' + child2[code3]['gnStockId'][2:] if len(
                                                child2[code3]['gnStockId']) > 0 else "None"
                                            gnStockName3 = child2[code3]['gnStockName']
                                            # 概念
                                            bkArr.append((gnStockId3, gnStockCode3, gnStockName3, identify3, code3,
                                                          code2, codeType, getCurTime(), 1, getCurTime()))
                                            # 股票池
                                            if 'stockpool' in child2[code3] and len(child2[code3]['stockpool']) > 0:
                                                instate3 = False
                                                if code3 in inout_old_arr:
                                                    instate3 = True

                                                for stockId3 in child2[code3]['stockpool']:
                                                    stockId3 = str(1000000 + int(stockId3))[1:]
                                                    stkId3 = stockToStk(stockId3)
                                                    stockArr.append((stockId3, stkId3, code3, getCurTime()))
                                                    if not instate3:
                                                        inoutArr.append((stockId3, stkId3, code3, 1, getCurTime()))

                                                if instate3:
                                                    # 在inout_old_arr中但不在stockpool中的标的:剔除标的
                                                    diff1 = list(
                                                        set(inout_old_arr[code3]) - set([int(x) for x in child2[code3]['stockpool']]))
                                                    for stockId in diff1:
                                                        inoutArr.append(
                                                            (str(1000000 + int(stockId))[1:], stockToStk(stockId), code3, 0, getCurTime()))

                                                    # 在stockpool中但不在inout_old_arr中标的:新加入标的
                                                    diff2 = list(
                                                        set([int(x) for x in child2[code3]['stockpool']]) - set(inout_old_arr[code3]))
                                                    for stockId in diff2:
                                                        inoutArr.append(
                                                            (str(1000000 + int(stockId))[1:], stockToStk(stockId), code3, 1, getCurTime()))

                                                    del inout_old_arr[code3]

                        del result

                        # 板块剔除: 剔除标的
                        if len(inout_old_arr) > 0:
                            for code in inout_old_arr:
                                for stockId in inout_old_arr[code]:
                                    inoutArr.append((str(1000000 + int(stockId))[1:], stockToStk(stockId), code, 0, getCurTime()))

                        # 概念股票池-股票进出
                        if len(inoutArr) > 0:
                            sql = "INSERT INTO `bk_stock_inout` (`stock_id`, `stk_id`, `parentcode`, `state`, `date_create`) VALUES " + \
                                  str(','.join(str(i) for i in inoutArr))
                            cursor.execute(sql)

                            inout_log = ""
                            for val in inoutArr:
                                inout_log = inout_log + str(val[0]) + ("加入" if int(val[3]) == 1 else "剔除") + str(val[2]) + ";"
                            logger.info('========= {} ========='.format(inout_log))
                            logger.info('========= 2.概念股票池-股票进出:数据更新完毕 {} ========='.format(getCurTime()))
                        else:
                            logger.info('========= 2.概念股票池-股票进出:源数据未更新 {} ========='.format(getCurTime()))

                        del inout_old_arr, inoutArr

                        # 概念池
                        if len(bkArr) > 0:
                            cursor.execute("select `code`,`state` FROM `bk_map`")
                            results = cursor.fetchall()
                            map_old_arr = []
                            map_old_data = {}
                            map_new_arr = []
                            for code, state in results:
                                code = str(code)
                                map_old_arr.append(code)
                                map_old_data[code] = state
                            for val in bkArr:
                                map_new_arr.append(str(val[4]))

                            # 相同概念 - 可能是之前已剔除的
                            rebirth_str = ""
                            comEle = list(set(map_old_arr) & set(map_new_arr))
                            comArr = []
                            for code0 in comEle:
                                if int(map_old_data[code0]) == 0:
                                    rebirth_str = rebirth_str + code0 + ";"
                                comArr.append("(`code` = '" + code0 + "')")
                            cursor.execute("UPDATE `bk_map` SET `state` = 1, `date_update` = '" + getCurTime() + "' WHERE " + str(' or '.join(str(i) for i in comArr)))

                            # 剔除概念
                            map_del_str = ""
                            delEle = list(set(map_old_arr) - set(map_new_arr))
                            if len(delEle) > 0:
                                delArr = []
                                for code1 in delEle:
                                    map_del_str = map_del_str + code1 + ";"
                                    delArr.append("(`code` = '" + code1 + "')")
                                cursor.execute("UPDATE `bk_map` SET `state` = 0, `date_update` = '" + getCurTime() + "' WHERE " + str(' or '.join(str(i) for i in delArr)))

                            # 新加入概念
                            map_add_str = ""
                            insertSql = "INSERT INTO `bk_map` (`stock_id`, `stock_code`, `stock_name`, `identify`, `code`, `parentcode`, `type`, `date_update`, `state`, `block_start_time`) VALUES "
                            addEle = list(set(map_new_arr) - set(map_old_arr))
                            if len(addEle) > 0:
                                insertArr = []
                                for code2 in addEle:
                                    map_add_str = map_add_str + code2 + ";"
                                    for v in bkArr:
                                        if code2 == str(v[4]):
                                            insertArr.append(v)
                                cursor.execute(insertSql + str(','.join(str(i) for i in insertArr)))

                            if len(map_add_str + map_del_str + rebirth_str) > 0:
                                logger.info('========= {} ========='.format("加入概念:" + map_add_str + "剔除概念:" + map_del_str + "重新生效概念:" + rebirth_str))
                            else:
                                logger.info('========= 概念池数据源无更新 =========')
                            logger.info('========= 3.概念池:数据更新完毕 {} ========='.format(getCurTime()))
                        else:
                            logger.info('========= 3.概念池:源数据异常 {} ========='.format(getCurTime()))

                        # 概念股票池
                        if len(stockArr) > 0:
                            cursor.execute("DELETE FROM `bk_stock_map`")
                            sql = "INSERT INTO `bk_stock_map` (`stock_id`, `stk_id`, `parentcode`, `date_create`) VALUES " + str(','.join(str(i) for i in stockArr))
                            cursor.execute(sql)
                            logger.info('========= 4.概念股票池:数据更新完毕 {} ========='.format(getCurTime()))
                        else:
                            logger.info('========= 4.概念股票池:源数据异常 {} ========='.format(getCurTime()))

                        del bkArr, stockArr
                    else:
                        logger.info('========= 2.概念及股票:源数据未更新 {} ========='.format(getCurTime()))
                    cnx.commit()
                else:
                    logger.warning('========= 2.概念及股票:文件异常 {} ========='.format(getCurTime()))
            except Exception as e:
                cnx.rollback()
                logger.debug('========= 2.概念及股票:程序异常({}) {} ========='.format(str(e), getCurTime()))

        except Exception as e:
            logger.debug('========= 程序异常({}) {} ========='.format(str(e), getCurTime()))
        finally:
            # 提交事务并关闭连接
            cursor.close()
            cnx.close()

        logger.info('========= grap end {} ========='.format(getCurTime()))


if __name__ == "__main__":
    # 日志记录
    fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(level=logging.INFO, format=fmt)
    format_str = logging.Formatter(fmt)
    logger = logging.getLogger()  # 获取⼀个logger实例
    handle = RotatingFileHandler(config.logname, maxBytes=10240000, backupCount=5, encoding="utf-8")  # 设置⽇志回滚
    handle.setFormatter(format_str)
    handle.namer = lambda x: config.logname + '.' + x.split('.')[-1]  # 设置⽇志名称
    logger.addHandler(handle)  # 给logger添加handler

    grap()

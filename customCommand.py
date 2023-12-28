from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django_bulk_update.helper import bulk_update
from django.utils import timezone
from t_search.models import info_config, LBkeyInfo
from housing_cases.models import (JrA, JrB, JrC, JrD, JrE, JrF, JrG, JrH, JrI, JrJ, JrK, JrM, JrN, JrO, JrP, JrQ, JrT, JrU, JrV, JrW, JrX, JrY, JrZ,
JzA, JzB, JzC, JzD, JzE, JzF, JzG, JzH, JzI, JzJ, JzK, JzM, JzN, JzO, JzP, JzQ, JzT, JzU, JzV, JzW, JzX, JzY, JzZ)
from lvr_land.models import HousingCases
from commons.util import batch

from datetime import timedelta
from functools import reduce
from tqdm import tqdm
import operator
import time
import sys
import logging
import json

logger = logging.getLogger(__name__)
MAX_COUNT = 3
MAX_BATCH_NUM = 1000
MIN_BATCH_NUM = 500

class Command(BaseCommand):
    """
    This command will print a command line argument.
    """
    help = 'This command will assign infos processed from inputs to BusinessDealers.'
    
    def add_arguments(self, parser):

        parser.add_argument(
            '-t',
            '--task_type',
            action='store',
            dest='task_type',
            default=None,
            help=''' input data'''
        )

        parser.add_argument(
            '-D',
            '--data',
            action='store',
            dest='data',
            default=None,
            help=''' input data'''
        )

        # 拉地建號清單用的參數
        parser.add_argument(
            '-c',
            '--city',
            action='store',
            dest='city',
            default=None,
            help=''' input city'''
        )

        # 指定lbtype
        parser.add_argument(
            '-lb',
            '--lbtype',
            action='store',
            dest='lbtype',
            default=None,
            help=''' input lbtype'''
        )

        parser.add_argument(
            '-cl',
            '--city_list',
            action='store',
            type=str, 
            nargs='+',
            # dest='city_list',
            default=None,
            help=''' input city_list'''
        )

    def update_main_data(self, con, ds_data_dict):
        try:
            start1 = time.perf_counter()
            update_data = HousingCases.objects.filter(reduce(operator.or_, con))
            use_package_id_list = []
            update_lists = []
            update_info_list = []
            lbkey_list = []
            lbkey_dict = {}
            for s, update_list in enumerate(update_data):
                source = update_list.source
                source_id = update_list.source_id
                package_id = source + ';' + source_id
                use_package_id_list.append(package_id)
                use_data = ds_data_dict[package_id]
                # update_list.source = use_data['source
                # update_list.source_id = use_data['source_id
                update_list.subject = use_data['subject']
                update_list.city = use_data['city']
                update_list.area = use_data['area']
                update_list.road = use_data['road']
                update_list.address = use_data['address']
                update_list.situation = use_data['situation']
                update_list.total = use_data['total']
                update_list.price_ave = use_data['price_ave']
                update_list.feature = use_data['feature']
                update_list.pattern = use_data['pattern']
                update_list.pattern1 = use_data['pattern1']
                update_list.total_ping = use_data['total_ping']
                update_list.building_ping = use_data['building_ping']
                update_list.att_ping = use_data['att_ping']
                update_list.public_ping = use_data['public_ping']
                update_list.land_ping = use_data['land_ping']
                update_list.house_age = use_data['house_age']
                update_list.house_age_v = use_data['house_age_v']
                update_list.floor_web = use_data['floor_web']
                update_list.floor = use_data['floor']
                update_list.total_floor = use_data['total_floor']
                update_list.house_num = use_data['house_num']
                update_list.blockto = use_data['blockto']
                update_list.house_type = use_data['house_type']
                update_list.manage_type = use_data['manage_type']
                update_list.manage_fee = use_data['manage_fee']
                update_list.edge = use_data['edge']
                update_list.dark = use_data['dark']
                update_list.parking_type = use_data['parking_type']
                update_list.lat = use_data['lat']
                update_list.lng = use_data['lng']
                update_list.link = use_data['link']
                update_list.img_url = use_data['img_url']
                update_list.contact_type = use_data['contact_type']
                update_list.contact_man = use_data['contact_man']
                update_list.phone = use_data['phone']
                update_list.brand = use_data['brand']
                update_list.branch = use_data['branch']
                update_list.company = use_data['company']
                update_list.price_renew = use_data['price_renew']
                # update_list.insert_time = use_data['insert_time']
                update_list.update_time = use_data['update_time']
                update_list.community = use_data['community']
                update_list.mrt = use_data['mrt']
                update_list.group_man = use_data['group_man']
                update_list.group_key = use_data['group_key']
                # update_list.sale_count = use_data['sale_count']
                update_list.group_record = use_data['group_record']
                update_list.history = use_data['history']
                update_list.address_cal = use_data['address_cal']
                update_list.is_delete = use_data['is_delete']
                update_list.is_hidden = use_data['is_hidden']

                #! 更新 LBkeyInfo 前置動作
                try:
                    check_data = json.loads(use_data['address_cal']) if use_data['address_cal'] else ''
                    bkey_datas = check_data['datas'] if check_data and 'datas' in check_data else []

                    update_date = timezone.localtime(use_data['update_time']).strftime("%Y-%m-%d")
                    valid = 0 if use_data['is_delete'] else 1
                    for bkey_data in bkey_datas:
                        bkey = bkey_data['bkey']
                        if bkey not in lbkey_dict or (bkey in lbkey_dict and (update_date > lbkey_dict[bkey]['case_data'])):
                            lbkey_list.append(bkey)
                            lbkey_dict[bkey] = {
                                'case_data': update_date,
                                'case_valid': valid
                            }
                except Exception as e:
                    print(f'錯誤訊息：{e}，錯誤行數：{sys.exc_info()[2].tb_lineno}')

                update_lists.append(update_list)
                if ((s+1) % MIN_BATCH_NUM) == 0 or (s+1) == len(update_data):
                    #! 更新 LBkeyInfo
                    # print(lbkey_list)
                    lbkey_infos = LBkeyInfo.objects.filter(lbkey__in=lbkey_list)
                    for lbkey_info in lbkey_infos:
                        lbkey = lbkey_info.lbkey
                        case_data = str(lbkey_info.case_data)
                        # print(f'建號：{lbkey}')
                        # print(f'原始：{case_data}')
                        # print(f'新的：{lbkey_dict[lbkey]["case_data"]}')
                        if case_data == 'None' or (lbkey_dict[lbkey]['case_data'] and lbkey_dict[lbkey]['case_data'] > case_data):
                            lbkey_info.case_data = lbkey_dict[lbkey]['case_data']
                            lbkey_info.case_valid = lbkey_dict[lbkey]['case_valid']
                            update_info_list.append(lbkey_info)
                    with transaction.atomic():
                        if len(update_lists):
                            bulk_update(update_lists, update_fields = ['subject', 'city', 'area', 'road', 'address', 'situation', 'total',
                            'price_ave', 'feature', 'pattern', 'pattern1', 'total_ping', 'building_ping', 'att_ping', 'public_ping',
                            'land_ping', 'house_age', 'house_age_v', 'floor_web', 'floor', 'total_floor', 'house_num', 'blockto',
                            'house_type', 'manage_type', 'manage_fee', 'edge', 'dark', 'parking_type', 'lat', 'lng', 'link', 'img_url',
                            'contact_type', 'contact_man', 'phone', 'brand', 'branch', 'company', 'price_renew', 'update_time', 'community',
                            'mrt', 'group_man', 'group_key', 'group_record', 'history','address_cal', 'is_delete', 'is_hidden'])
                        if update_info_list:
                            # print(f'更新筆數：{len(update_info_list)}')
                            LBkeyInfo.objects.bulk_update(update_info_list, fields=['case_data', 'case_valid'])
                        update_lists = []
                        update_info_list = []
                        lbkey_list = []
                        lbkey_dict = {}
                # if (s+1) == len(update_data):
                #     with transaction.atomic():
                #         if len(update_lists):
                #             bulk_update(update_lists, update_fields = ['subject', 'city', 'area', 'road', 'address', 'situation', 'total',
                #             'price_ave', 'feature', 'pattern', 'pattern1', 'total_ping', 'building_ping', 'att_ping', 'public_ping',
                #             'land_ping', 'house_age', 'house_age_v', 'floor_web', 'floor', 'total_floor', 'house_num', 'blockto',
                #             'house_type', 'manage_type', 'manage_fee', 'edge', 'dark', 'parking_type', 'lat', 'lng', 'link', 'img_url',
                #             'contact_type', 'contact_man', 'phone', 'brand', 'branch', 'company', 'price_renew', 'update_time', 'community',
                #             'mrt', 'group_man', 'group_key', 'group_record', 'history','address_cal', 'is_delete', 'is_hidden'])
                #         if update_info_list:
                #             print(f'更新筆數：{len(update_info_list)}')
                #             LBkeyInfo.objects.bulk_update(update_info_list, fields=['recno_date', 'case_valid'])
            end1 = time.perf_counter()
            # print('更新主資料：', end1 - start1)
            return use_package_id_list
        except Exception as e:
            logger.info(f'錯誤訊息：{e}，錯誤行數：{sys.exc_info()[2].tb_lineno}')
            return use_package_id_list

    def bulk_create_data(self, package_id_list, ds_data_dict):
        start1 = time.perf_counter()
        creat_list = []
        for ss, package in enumerate(package_id_list):
            data_create = HousingCases(**ds_data_dict[package])
            creat_list.append(data_create)
            if ((ss+1) % MIN_BATCH_NUM) == 0:
                with transaction.atomic():
                    if len(creat_list):
                        HousingCases.objects.bulk_create(creat_list, ignore_conflicts=True)
                    creat_list = []
            if (ss+1) == len(package_id_list):
                with transaction.atomic():
                    if len(creat_list):
                        HousingCases.objects.bulk_create(creat_list, ignore_conflicts=True)
        end1 = time.perf_counter()
        # print('創建新資料：', end1 - start1)

    def update_sale_count(self, group_key_list):
        start1 = time.perf_counter()
        update_list = []
        data_dict = {}
        group_key_data = HousingCases.objects.filter(group_key__in=group_key_list, is_delete=False)
        for i in group_key_data:
            if i.group_key in data_dict:
                data_dict[i.group_key] += 1
            else:
                data_dict[i.group_key] = 1

        for s, data in enumerate(group_key_data):
            data.sale_count = data_dict[data.group_key]
            update_list.append(data)
            if ((s+1) % MIN_BATCH_NUM) == 0:
                with transaction.atomic():
                    if len(update_list):
                        bulk_update(update_list, update_fields = ['sale_count'])
                    update_list = []
            if (s+1) == len(group_key_data):
                with transaction.atomic():
                    if len(update_list):
                        bulk_update(update_list, update_fields = ['sale_count'])
                    update_list = []
        end1 = time.perf_counter()
        # print('更新sale_count：', end1 - start1)

    def main_process(self, data, datas):
        if not data:
            print(datas, ' 無更新資料')
            return ''
        con = []
        ds_data_dict = {}
        group_key_list = []
        total_package_id_list = []
        # 新增部份
        # progress = tqdm(total=len(data))
        for s, ds in enumerate(data):
            # progress.update(1)
            # 統計要更新的 group_key 清單
            group_key = ds.group_key
            if group_key and not ds.is_delete:
                group_key_list.append(group_key)
            # 組 package_id 清單
            source = ds.source
            source_id = ds.source_id
            package_id = source + ';' + source_id
            total_package_id_list.append(package_id)
            try:
                # 主資料清單
                ds_data_dict[package_id] = {
                                            'source': source,
                                            'source_id': source_id,
                                            'subject': ds.subject,
                                            'city': ds.city,
                                            'area': ds.area,
                                            'road': ds.road,
                                            'address': ds.address,
                                            'situation': ds.situation,
                                            'total': ds.total,
                                            'price_ave': ds.price_ave,
                                            'feature': ds.feature,
                                            'pattern': ds.pattern,
                                            'pattern1': ds.pattern1,
                                            'total_ping': ds.total_ping,
                                            'building_ping': ds.building_ping,
                                            'att_ping': ds.att_ping,
                                            'public_ping': ds.public_ping,
                                            'land_ping': ds.land_ping,
                                            'house_age':ds.house_age,
                                            'house_age_v':ds.house_age_v,
                                            'floor_web': ds.floor_web,
                                            'floor': ds.floor,
                                            'total_floor': ds.total_floor,
                                            'house_num': ds.house_num,
                                            'blockto': ds.blockto,
                                            'house_type': ds.house_type,
                                            'manage_type': ds.manage_type,
                                            'manage_fee': ds.manage_fee,
                                            'edge': ds.edge,
                                            'dark': ds.dark,
                                            'parking_type': ds.parking_type,
                                            'lat': ds.lat,
                                            'lng': ds.lng,
                                            'link': ds.link,
                                            'img_url': ds.img_url,
                                            'contact_type': ds.contact_type,
                                            'contact_man': ds.contact_man,
                                            'phone': ds.phone,
                                            'brand': ds.brand,
                                            'branch': ds.branch,
                                            'company': ds.company,
                                            'price_renew': ds.price_renew,
                                            'insert_time': ds.insert_time,
                                            'update_time': ds.update_time,
                                            'community': ds.community,
                                            'mrt': ds.mrt,
                                            'group_man': ds.group_man,
                                            'group_key': group_key,
                                            'sale_count': 1,
                                            'group_record': ds.group_record,
                                            'history': ds.history,
                                            'address_cal': ds.address_cal,
                                            'is_delete': ds.is_delete,
                                            'is_hidden': ds.is_hidden
                                            }
                # 組更新搜尋
                q1 = Q()
                q1.connector = 'AND'
                q1.children.append(("source", source))
                q1.children.append(("source_id", source_id))
                con.append(q1)

                # 更新部份
                if ((s+1) % MAX_BATCH_NUM) == 0:
                    start3 = time.perf_counter()
                    if con:
                        use_package_id_list = self.update_main_data(con, ds_data_dict)
                    else:
                        use_package_id_list = []
                    package_id_list = list(set(total_package_id_list) - set(use_package_id_list))
                    if package_id_list:
                        self.bulk_create_data(package_id_list, ds_data_dict)
                    if group_key_list:
                        self.update_sale_count(group_key_list)
                    end3 = time.perf_counter()
                    print('資料庫匯入總處理時間：', end3 - start3)
                    # 初始化資料
                    con = []
                    ds_data_dict = {}
                    group_key_list = []
                    total_package_id_list = []
                if (s+1) == len(data):
                    start3 = time.perf_counter()
                    if con:
                        use_package_id_list = self.update_main_data(con, ds_data_dict)
                    else:
                        use_package_id_list = []
                    package_id_list = list(set(total_package_id_list) - set(use_package_id_list))
                    if package_id_list:
                        self.bulk_create_data(package_id_list, ds_data_dict)
                    if group_key_list:
                        self.update_sale_count(group_key_list)
                    end3 = time.perf_counter()
                    # print('資料庫匯入總處理時間：', end3 - start3)
            except Exception as e:
                # print(ds_data_dict)
                print(package_id)
                print(e, 'exception in line', sys.exc_info()[2].tb_lineno)

    def handle(self, *args, **options):
        # 案源資料匯入
        if options['task_type'] == 'RS':
            print('案源資料匯入')
            start1 = time.perf_counter()
            try:
                data_list = [JrA, JrB, JrC, JrD, JrE, JrF, JrG, JrH, JrI, JrJ, JrK, JrM, JrN, JrO, JrP, JrQ, JrT, JrU, JrV, JrW, JrX, JrY, JrZ,
                            JzA, JzB, JzC, JzD, JzE, JzF, JzG, JzH, JzI, JzJ, JzK, JzM, JzN, JzO, JzP, JzQ, JzT, JzU, JzV, JzW, JzX, JzY, JzZ]
                #! 測試用
                # data_list = [JrA]
                hcbm = info_config.objects.get(lbtype='hcbm_date')
                hcbm_time = hcbm.create_time
                now_time = timezone.now() + timedelta(hours=+8)
                for datas in data_list:
                    print('大類表：', datas)
                    low = 0
                    hight = 500000
                    start = time.perf_counter()
                    data_lens = len(datas.objects.filter(update_time__gt=hcbm_time).values('is_delete'))
                    end = time.perf_counter()
                    print(f'第一段：{end - start}')
                    if data_lens > 500000:
                        d = int(data_lens / 500000) + 1
                        for s in range(d):
                            # print('大類表：', datas, ' 第{}輪'.format(str(s+1)))
                            if low != 0:
                                low *= s
                            data = datas.objects.filter(update_time__gt=hcbm_time)[low:hight * (s+1)]
                            process = self.main_process(data, datas)
                            if low == 0:
                                low = 500000
                    else:
                        start = time.perf_counter()
                        data = datas.objects.filter(update_time__gt=hcbm_time)
                        process = self.main_process(data, datas)
                        end = time.perf_counter()
                        print(f'第二段：{end - start}')

                hcbm.create_time = now_time
                hcbm.save()
                end1 = time.perf_counter()
                print('案源更新匯入完成，總執行時間：', end1 - start1)
                # logger.info('案源更新匯入完成，總執行時間：{}'.format(end1 - start1))
            except Exception as e:
                print(e, 'exception in line', sys.exc_info()[2].tb_lineno)
                end1 = time.perf_counter()
                print('案源匯入失敗，總執行時間：', end1 - start1)
                # logger.info('案源匯入失敗，總執行時間：{}'.format(end1 - start1))

        #! 案源資料時間首次匯入lbkey_info
        if options['task_type'] == 'PK':
            start1 = time.perf_counter()
            print('開始執行案源時間匯入')
            try:
                # city_list = ['基隆市', '台北市', '新北市', '桃園市', '新竹市', '新竹縣', '苗栗縣', '台中市', '彰化縣', '南投縣', '雲林縣', '嘉義市', '嘉義縣', '台南市', '高雄市', '屏東縣', '台東縣', '花蓮縣', '宜蘭縣']
                city = options['city']

                #! 案源資料
                lbkey_datas = HousingCases.objects.filter(city=city).values('update_time' ,'address_cal', 'is_delete')
                progress = tqdm(total=len(lbkey_datas))
                for datas in batch(lbkey_datas, n=5000):
                    lbkey_list = []
                    data_dict = {}
                    for data in datas:
                        update_date = timezone.localtime(data['update_time']).strftime("%Y-%m-%d")
                        valid = 0 if data['is_delete'] else 1
                        bkey_str = json.loads(data['address_cal']) if data['address_cal'] else ''
                        bkey_datas = bkey_str['datas'] if bkey_str and 'datas' in bkey_str else []
                        for bkey_data in bkey_datas:
                            bkey = bkey_data['bkey']
                            if bkey not in data_dict or (bkey in data_dict and (update_date > data_dict[bkey]['case_data'])):
                                lbkey_list.append(bkey)
                                data_dict[bkey] = {
                                    'case_data': update_date,
                                    'case_valid': valid
                                }

                    #! 更新LBkeyInfo
                    update_list = []
                    lbkey_infos = LBkeyInfo.objects.filter(lbkey__in=lbkey_list)
                    for lbkey_info in lbkey_infos:
                        lbkey = lbkey_info.lbkey
                        tr_date = str(lbkey_info.case_data)
                        if tr_date == 'None' or (data_dict[lbkey]['case_data'] and data_dict[lbkey]['case_data'] > tr_date):
                            lbkey_info.case_data = data_dict[lbkey]['case_data']
                            lbkey_info.case_valid = data_dict[lbkey]['case_valid']
                            update_list.append(lbkey_info)

                    with transaction.atomic():
                        if update_list:
                            tqdm.write(f'更新筆數：{len(update_list)}')
                            LBkeyInfo.objects.bulk_update(update_list, fields=['case_data', 'case_valid'])
                    progress.update(5000)
                    # exit('測試結束')
                progress.close()
                end1 = time.perf_counter()
                print('案源時間匯入完成，總執行時間：',end1 - start1)
            except Exception as e:
                print(f'錯誤訊息：{e}，錯誤行數：{sys.exc_info()[2].tb_lineno}')
                end1 = time.perf_counter()
                print('案源時間匯入失敗，總執行時間：',end1 - start1)

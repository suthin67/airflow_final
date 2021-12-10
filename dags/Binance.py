# Required package
# python-binance==1.0.15
# binance-connector-python==0.6.0
# gspread==4.0.1
# pandas==1.2.4
# oauth2client==4.1.3
# numpy==1.20.2
# pyparsing==2.4.7
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from binance.client import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


def refresh_price(event, context):
    ##### Configulation for  google ######
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    # cerds = ServiceAccountCredentials.from_json_keyfile_name("cerds.json", scope)
    # gg_client = gspread.authorize(cerds)

    ## ขอ ข้้อมูลนี้ จาก google ก่อน
    json_data = {
      "type": "service_account",
      "project_id": "cypto-python",  ## ขอ project  id จาก google ก่อน
      "private_key_id": "{Google_Private_key_id}", ## ขอ Google_Private_key จาก google ก่อน
      "private_key": "{Google_Private_key}", ## ขอ Google_Private_key จาก google ก่อน
      "client_email": "cypto-python@cypto-python.iam.gserviceaccount.com",
      "client_id": "xxxx",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/cypto-python%40cypto-python.iam.gserviceaccount.com"
    }

    cerds = ServiceAccountCredentials.from_json_keyfile_dict(json_data, scope)
    gg_client = gspread.authorize(cerds)

    ##### Config for binance #######
    api_key = 'QqxfBBYKpzrbQars02Y4oz4jQwBAOMwtpYHozPtU6cQ6Zk1SnhVlT9aR2IiKRGKc'  # binance API Key
    api_secret = 'e39F0A8YTmqPVosT9RFzd0IysPBWgYXAgXM44ur9Uija4jPZaUb5XT5GJEpUT2tZ'  # binance API Secret
    client = Client(api_key, api_secret)

    ticker_ind = 1
    test_ind = 0
    backup_ind = 1
    if test_ind == 1:
        cloud = 0
    else:
        cloud = 1

    if test_ind != 1:
        sheet_name = 'Main'
        book_name = 'Crypto_price_24hrs'
    else:
        sheet_name = 'Main'
        book_name = 'Crypto_test'
    spreadsheet = gg_client.open(book_name)
    sheet = spreadsheet.worksheet(sheet_name)

    ####### function #############
    def date_cal(d):
        #dt = datetime.fromtimestamp(d/1000).strftime('%Y-%m-%d %H:%M:%S')
        dat = datetime.fromtimestamp(d / 1000)
        if cloud == 1:
            dat = dat + timedelta(hours=7)
        return dat.strftime('%Y-%m-%d %H:%M:%S')


    ############ Ticker ################
    if ticker_ind == 1:
        # Retrieve ticker
        tickers = client.get_ticker()
        df_ticker = pd.DataFrame(tickers)

        # # drop unused columns
        # df_ticker.drop(['firstId', 'lastId', 'count'], axis=1, inplace=True)

        # convert object to float
        for col in df_ticker.columns:
            if col not in ['symbol']:
                df_ticker[col] = pd.to_numeric(df_ticker[col], errors='coerce')
        print(df_ticker.dtypes)

        # calculate high per low price
        df_ticker['%High/LowPrice'] = 0
        df_ticker.loc[df_ticker['lowPrice'] != 0, '%High/LowPrice' ] = df_ticker['highPrice'] / df_ticker['lowPrice'] - 1

        # calculate %lastPrice to H/L
        df_ticker['%lastPrice to H/L'] = 0
        df_ticker.loc[(df_ticker['highPrice']-df_ticker['lowPrice'] ) != 0, '%lastPrice to H/L'] = (df_ticker['lastPrice'] - df_ticker['lowPrice'])/(df_ticker['highPrice'] - df_ticker['lowPrice'])

        # Convert to date format
        df_ticker['openTime'] = df_ticker['openTime'].apply(date_cal)
        df_ticker['closeTime'] = df_ticker['closeTime'].apply(date_cal)

        # choose volume or quote-volume
        df_ticker['VolumeUSDT'] = 0
        #df_ticker.loc[df_ticker.symbol[0:4] == 'USDT', 'Volume (Million USDT)'] = df_ticker.loc[df_ticker.symbol[0:4] == 'USDT', 'volume']
        #df_ticker.loc[df_ticker['symbol'][0:4] != 'USDT', 'Volume (Million USDT)'] = df_ticker.loc[df_ticker['symbol'][0:4] == 'USDT', 'quoteVolume']
        df_ticker['quote_ind'] = 1
        df_ticker.loc[df_ticker['symbol'].str[:4] == 'USDT', 'quote_ind'] = 0
        df_ticker['VolumeUSDT'] = np.where(df_ticker.quote_ind.eq(0), df_ticker.volume, df_ticker.quoteVolume)

        # add indicator
        df_ticker['big_volume'] = 0
        df_ticker.loc[(df_ticker['VolumeUSDT'] >= 10**8), 'big_volume'] = 1

        df_ticker['USDT'] = 0
        df_ticker.loc[df_ticker['symbol'].str.contains("USDT", case=False), 'USDT'] = 1
        # df_ticker.loc[df_ticker['symbol'].str[-4:] == 'USDT', 'USDT'] = 1

        df_ticker['positive_change'] = 0
        df_ticker.loc[(df_ticker['priceChange'] > 0), 'positive_change'] = 1

        # adjust data
        df_ticker['priceChangePercent'] = df_ticker['priceChangePercent']/100
        df_ticker['Vol (mUSDT)'] = df_ticker['VolumeUSDT'] / (10**6)

        # sorted by volume
        # df_ticker.sort_values(by='volume', ascending=False, inplace=True)

        # sorted by indicator
        df_ticker.sort_values(by=['USDT', 'big_volume', 'positive_change', '%High/LowPrice'], ascending=False, inplace=True)

        # delete non USDT
        df_ticker = df_ticker.drop(df_ticker[df_ticker.USDT == 0].index)

        # count interested row from indicator
        #count_good = df_ticker[(df_ticker['big_volume'] > 0) & (df_ticker['USDT'] > 0) & (df_ticker['positive_change'] == 1) ].symbol.count()
        count_good = df_ticker[(df_ticker['big_volume'] > 0) & (df_ticker['positive_change'] == 1)].symbol.count()

        # Rename column
        # df_ticker.rename(columns={'closeTime': 'lastUpdateTime', 'quoteVolume': 'quoteVolume (Million USDT)'}, inplace=True)
        rename_dict = {'symbol': 'symbol',
        'lastPrice': 'lastPrice',
        'Volume (Million USDT)': 'Vol (mUSDT)',
        '%highPrice_to_lowPrice': '%High/LowPrice',
        'highPrice': 'highPrice',
        'lowPrice': 'lowPrice',
        'priceChange': 'priceChg',
        'priceChangePercent': '%priceChg',
        'weightedAvgPrice': 'weightedAvgPrice',
        'openPrice': 'openPrice',
        'closeTime': 'lastUpdate'}
        df_ticker.rename(columns=rename_dict, inplace=True)
        df_line = df_ticker.copy()

        # drop indicators columns
        # df_ticker.drop(['big_volume', 'USDT'], axis=1, inplace=True)

        columns_need_dict = {  # python = done in python, excel = cal in google spreadsheet
            'symbol':'python',
            '%High/LowPrice':'python',
            '%priceChg':'python',
            '%lastPrice to H/L': 'python',
            'Vol (mUSDT)':'python',
            '%VolChg_24hrs':'excel_vol',
            '%VolChg_20hrs':'excel_vol',
            '%VolChg_16hrs':'excel_vol',
            '%VolChg_12hrs':'excel_vol',
            '%VolChg_8hrs':'excel_vol',
            '%VolChg_4hrs':'excel_vol',
            '%VolChg_3hrs': 'excel_vol',
            '%VolChg_2hrs': 'excel_vol',
            '%VolChg_1hrs':'excel_vol',
            '%priceChg_4hrs': 'excel_price',
            '%priceChg_3hrs': 'excel_price',
            '%priceChg_2hrs': 'excel_price',
            '%priceChg_1hrs':'excel_price',
            'lowPrice':'python',
            'lastPrice':'python',
            'highPrice':'python',
            'priceChg':'python',
            'openPrice':'python',
            'lastUpdate':'python'
        }
        # columns_need_list = ['symbol', '%High/LowPrice', '%priceChg', 'Vol (mUSDT)', '%VolChg_24hrs', '%VolChg_20hrs',
        #                 '%VolChg_16hrs', '%VolChg_12hrs', '%VolChg_8hrs', '%VolChg_4hrs', '%VolChg_1hrs',
        #                 '%priceChg_1hr', 'lowPrice', 'lastPrice', 'highPrice', 'priceChg', 'openPrice', 'lastUpdate']
        columns_need_list = list(columns_need_dict.keys())

        # To add missing column as 0
        Missing_col = []
        for col in columns_need_list:
            if col not in list(df_ticker.columns):
                Missing_col.append(col)
        print('Missing columns:')
        print(Missing_col)
        Blank_df = pd.DataFrame(columns=Missing_col)
        df_ticker = pd.concat([df_ticker, Blank_df], axis=1)
        df_ticker[Missing_col] = 0

        # select/re-order columns
        df_ticker = df_ticker[columns_need_list]

        # count row of new data
        data_cnt = len(df_ticker)
        print('Data count: ' + str(data_cnt))

        # last_col
        last_col = 'X'


        # backup sheet
        if backup_ind == 1:
            # paste as value
            sourceSheetName = sheet_name
            destinationSheetName = sheet_name
            sourceSheetId = spreadsheet.worksheet(sourceSheetName)._properties['sheetId']
            destinationSheetId = spreadsheet.worksheet(destinationSheetName)._properties['sheetId']
            body = {
                "requests": [
                    {
                        "copyPaste": {
                            "source": {
                                "sheetId": sourceSheetId,
                                "startRowIndex": 1,
                                "endRowIndex": data_cnt + 1,
                                "startColumnIndex": 5,
                                "endColumnIndex": 15
                            },
                            "destination": {
                                "sheetId": destinationSheetId,
                                "startRowIndex": 1,
                                "endRowIndex": data_cnt + 1,
                                "startColumnIndex": 5,
                                "endColumnIndex": 15
                            },
                            "pasteType": "PASTE_VALUES"
                        }
                    }
                ]
            }
            res = spreadsheet.batch_update(body)
            print(res)

            if cloud == 1:
                updateTime = datetime.now() + timedelta(hours=7)
            else:
                updateTime = datetime.now()
            updateTime_hr = updateTime.strftime('%H')

            if int(updateTime_hr) == 0:
                hist_hr = 23
            else:
                hist_hr = int(updateTime_hr) - 1

            hist_sheet_name = 'H' + str(hist_hr)

            try: # delete old backup sheet if it is exist
                hist_sheet = gg_client.open(book_name).worksheet(hist_sheet_name)
                gg_client.open(book_name).del_worksheet(hist_sheet)
            except:
                pass

            data_sheet_id = gg_client.open(book_name).worksheet(sheet_name).id
            gg_client.open(book_name).duplicate_sheet(source_sheet_id=data_sheet_id, insert_sheet_index=30, new_sheet_name=hist_sheet_name)

            hist_sheet = gg_client.open(book_name).worksheet(hist_sheet_name)
            # hist_sheet.delete_columns(12, 19) # Column L to S

        # df_ticker.to_csv('D:\Python\Project\\test.csv')

        # clear old data
        # print(book_name)
        # print(sheet_name)
        sheet.clear()

        # write to spreadsheet
        sheet.update([df_ticker.columns.values.tolist()] + df_ticker.values.tolist())

        # clear and set filter
        sheet.clear_basic_filter()
        sheet.set_basic_filter(name=(f'A:' + last_col))

        # highlight good crypto
        sheet.format("B2:C" + str(count_good+1), {
        "backgroundColor": { # blue
          "red": 0.0,
          "green": 1.0,
          "blue": 1.0
        }
        })

        # Add background row for the next 20 rows after good crypto row
        for i in range(2, 20):
            if i != 0:
                if (count_good + i + 1) % 2 == 0:
                    color = 20.0  # grey
                else:
                    color = 1.0   # white

                sheet.format("A" + str(count_good + i) + ":" + last_col + str(count_good + i), {
                "backgroundColor": {
                  "red": color,
                  "green": color,
                  "blue": color
                }
                })

        # find columns that are calculated in excel
        col_excel_vol_list = []
        colno_excel_list = []
        for (key, value) in columns_need_dict.items():
            # Check if value is 'excel_vol', if so get the index and and find the column alphabet and put in the list
            if value == 'excel_vol':
                col_no = columns_need_list.index(key) + 1
                col_alphabet = chr(ord('@') + col_no)
                col_excel_vol_list.append(col_alphabet)
                colno_excel_list.append(col_no)
        print(col_excel_vol_list)

        col_excel_price_list = []
        for (key, value) in columns_need_dict.items():
            # Check if value is 'excel_price', if so get the index and and find the column alphabet and put in the list
            if value == 'excel_price':
                col_no = columns_need_list.index(key) + 1
                col_alphabet = chr(ord('@') + col_no)
                col_excel_price_list.append(col_alphabet)
                colno_excel_list.append(col_no)
        print(col_excel_price_list)
        print(colno_excel_list)

        # delete value 0 from spreadsheet to prevent arrayformula error
        spreadsheet.values_clear(sheet_name + '!F2:R' + str(data_cnt + 1))

        # Add formula to calculate in spreadsheet
        # for volume change columns
        for i, col_str in enumerate(col_excel_vol_list):
            if col_str != 'N':
                next_col_str = col_excel_vol_list[col_excel_vol_list.index(col_str) + 1]
                sheet.update_acell(col_str + '2', '=ARRAYFORMULA(IFERROR(VLOOKUP($A$2:$A$' + str( data_cnt + 1)
                + ',INDIRECT("\'" & VLOOKUP(' + next_col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$A:$' + last_col +'"),MATCH("Vol (mUSDT)",INDIRECT("\'" & VLOOKUP(' + next_col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$1:$1"),0),0)/VLOOKUP($A$2:$A$' + str(data_cnt + 1)
                + ',INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$A:$' + last_col +'"),MATCH("Vol (mUSDT)",INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$1:$1"),0),0)-1,""))')

                # sheet.update_acell(col_str + '2', '=ARRAYFORMULA(IFERROR($D$2:$D$' + str(data_cnt + 1) + '/VLOOKUP($A$2:$A$' + str(
                #     data_cnt + 1) + ',INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$A:$X"),MATCH("Vol (mUSDT)",INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$1:$1"),0),0)-1,""))')

        col_str = 'N'
        sheet.update_acell(col_str + '2', '=ARRAYFORMULA(IFERROR($E$2:$E$' + str( data_cnt + 1) + '/VLOOKUP($A$2:$A$' + str(data_cnt + 1)
        + ',INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$A:$' + last_col +'"),MATCH("Vol (mUSDT)",INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$C$4:$E$28,3,0) &"\'!$1:$1"),0),0)-1,""))')
        # =ARRAYFORMULA($D$2:$D$356/VLOOKUP($A$2:$A$356,INDIRECT("'" & VLOOKUP(M$1,Time!$C$4:$E$28,3,0) &"'!$A:$R"),MATCH($D$1,INDIRECT("'" & VLOOKUP(M$1,Time!$C$4:$E$28,3,0) &"'!$1:$1"),0),0)-1)


        # for price change
        for col_str in col_excel_price_list:
            if col_str != 'R':
                next_col_str = col_excel_price_list[col_excel_price_list.index(col_str) + 1]
                sheet.update_acell(col_str + '2','=ARRAYFORMULA(IFERROR(VLOOKUP($A2:$A' + str(data_cnt + 1)
                                   + ',INDIRECT("\'" & VLOOKUP(' + next_col_str + '$1,Time!$D$4:$E$28,2,0) &"\'!$A:$' + last_col
                                   +'"),MATCH("lastPrice",INDIRECT("\'" & VLOOKUP(' + next_col_str + '$1,Time!$D$4:$E$28,2,0) &"\'!$1:$1"),0),0)/VLOOKUP($A2:$A'
                                   + str(data_cnt + 1) + ',INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$D$4:$E$28,2,0) &"\'!$A:$' + last_col
                                   +'"),MATCH("lastPrice",INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$D$4:$E$28,2,0) &"\'!$1:$1"),0),0)-1,""))')

                # =ARRAYFORMULA(IFERROR(VLOOKUP($A2:$A356,INDIRECT("'" & VLOOKUP(O$1,Time!$D$4:$E$28,2,0) &"'!$A:$X"),MATCH("lastPrice",INDIRECT("'" & VLOOKUP(O$1,Time!$D$4:$E$28,2,0) &"'!$1:$1"),0),0)/VLOOKUP($A2:$A356,INDIRECT("'" & VLOOKUP(N$1,Time!$D$4:$E$28,2,0) &"'!$A:$X"),MATCH("lastPrice",INDIRECT("'" & VLOOKUP(N$1,Time!$D$4:$E$28,2,0) &"'!$1:$1"),0),0)-1,""))

        col_str = 'R'
        sheet.update_acell(col_str + '2', '=ARRAYFORMULA(IFERROR($T2:$T' + str(data_cnt + 1) + '/VLOOKUP($A2:$A' + str(data_cnt + 1) + ',INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$D$4:$E$28,2,0) &"\'!$A:$' + last_col +'"),MATCH("lastPrice",INDIRECT("\'" & VLOOKUP(' + col_str + '$1,Time!$D$4:$E$28,2,0) &"\'!$1:$1"),0),0)-1,""))')
        # ARRAYFORMULA($S2:$S356/VLOOKUP($A2:$A356,INDIRECT("'" & VLOOKUP(Q$1,Time!$D$4:$E$28,2,0) &"'!$A:$X"),MATCH("lastPrice",INDIRECT("'" & VLOOKUP(Q$1,Time!$D$4:$E$28,2,0) &"'!$1:$1"),0),0)-1)

        # Add Heading
        # cell_list = sheet.range('L1:Q1')
        # cell_values = [k * 4 for k in range(1, 7)]
        # for i, val in enumerate(cell_values): #gives us a tuple of an index and value
        #     cell_list[i].value = '%Vol_Chg_from_' + str(val) + 'hrs_ago'  # use the index on cell_list and the val from cell_values
        # sheet.update_cells(cell_list)
        # sheet.update_acell('R1', '%price_change_1hr')
        # sheet.update_acell('S1', '%Vol_Chg_from_1hrs_ago')

        # wait for calculation
        time.sleep(2)

        # line notification
        if int(updateTime_hr) % 2 == 0:
            line = line_notification(df_line)
            print(line)
        else:
            print('Outside line notification time')

        # # Get the formula value from the souce cell:
        # formula = wks.acell('A2', value_render_option='FORMULA').value
        #
        # # Update the target cell with formula:
        # wks.update_acell('B3', formula)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    refresh_price(0, 0)
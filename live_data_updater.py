import websocket
import threading
    
import requests
import json
import sys
import os
import gc   
import time

from datetime import datetime
import time

full_restart_request = False
data_manager = None

kline_sec_duration = 300

def handle_exception(fired_exception, comment):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(comment, fired_exception)
    print(exc_type, fname, exc_tb.tb_lineno, "\n\n")
    sys.stdout.flush()


class binance_symbol_data:
    def __init__(self):
        #new kline data is in the end of lists
        self.represented_symbol = ""

        self.updates_banned = 0
        self.last_btc_price = 0

        self.current_kline_update_json = None

        self.timestamp = []
        self.open = []
        self.high = []
        self.low = []
        self.close = []
        self.deals_num = []
        self.total_quote_vol = []
        self.buy_quote_vol = []

        self.total_base_vol = []
        self.buy_base_vol = []

    def process_unclosed_kline(self):
        try:
            kline_timestamp = 0
            kline_data = None
            if self.current_kline_update_json != None:
                kline_data = self.current_kline_update_json['data']['k']
                kline_timestamp = int(int(kline_data["t"])/1000)

            if kline_timestamp <= self.timestamp[-1]:
                self.drop_rearmost_kline_data()

                self.timestamp.append(self.timestamp[-1]+300)
                self.open.append(self.open[-1])
                self.high.append(self.open[-1])
                self.low.append(self.open[-1])
                self.close.append(self.open[-1])
                self.deals_num.append(0)
                self.total_quote_vol.append(0)
                self.buy_quote_vol.append(0)

                self.total_base_vol.append(0)
                self.buy_base_vol.append(0)
            else:    
                self.process_new_closed_kline(self.current_kline_update_json)
        except Exception as e:
            handle_exception(e, "unclosed processing")

    def process_new_closed_kline(self, kline):
        try:
            kline_data = kline["data"]["k"]
            kline_symbol = kline["data"]["s"]
            kline_timestamp = int(kline_data["t"]/1000)

            if self.timestamp[-1] == kline_timestamp:
                self.drop_most_fresh_kline_data() 
            else:
                self.drop_rearmost_kline_data()

            self.timestamp.append(kline_timestamp)
            self.open.append(float(kline_data["o"]))
            self.high.append(float(kline_data["h"]))
            self.low.append(float(kline_data["l"]))
            self.close.append(float(kline_data["c"]))
            self.deals_num.append(float(kline_data["n"]))

            multiplyer = 1
            if self.represented_symbol[-3:] == "BTC":
                multiplyer = self.last_btc_price

            self.total_quote_vol.append(float(kline_data["q"]))
            self.buy_quote_vol.append(float(kline_data["Q"]))

            self.total_base_vol.append(float(kline_data["v"]) * multiplyer)
            self.buy_base_vol.append(float(kline_data["V"]) * multiplyer)

        except Exception as e:
            handle_exception(e, "process_new_closed_kline")   

    def drop_rearmost_kline_data(self):
        self.timestamp.pop(0)
        self.open.pop(0)
        self.high.pop(0)
        self.low.pop(0)
        self.close.pop(0)
        self.deals_num.pop(0)
        self.total_quote_vol.pop(0)
        self.buy_quote_vol.pop(0)
        self.total_base_vol.pop(0)
        self.buy_base_vol.pop(0)

    def drop_most_fresh_kline_data(self):
        self.timestamp.pop(-1)
        self.open.pop(-1)
        self.high.pop(-1)
        self.low.pop(-1)
        self.close.pop(-1)
        self.deals_num.pop(-1)
        self.total_quote_vol.pop(-1)
        self.buy_quote_vol.pop(-1)
        self.total_base_vol.pop(-1)
        self.buy_base_vol.pop(-1)


class binance_ws_data_collector:
    def __init__(self):
        #TODO add ignore symbols list

        self.ws_url_base = "wss://stream.binance.com:9443/stream?streams="
        self.ws_url_symbol_addon = "@kline_5m/"

        self.ws_connection = websocket.WebSocketApp("",
                                                      on_message = on_message,
                                                      on_error = on_error,
                                                      on_close = on_close)

        self.ws_thread = None
        self.uptime_hours = 0
        self.updates_after_cleaning = 0

        self.banned_changed = False
        self.served_symbols = set()
        self.day_ban_set = set()

        self.symbol_data_vault = {}
        self.symbol_klines_load_needed = []

        self.btc_time_price_dict = {}
        self.last_btc_price = 0

    def get_binance_available_pairs(self): #pairs with BTC and USDT

        get_assets_url = "https://api.binance.com/api/v1/exchangeInfo"
        list_to_return = []

        try:
            for i in range(5):
                try:
                    response = requests.get(get_assets_url, timeout=2)
                    parsed_json = json.loads(response.content)
                    symbols = parsed_json['symbols']
                    break
                except Exception as e:
                    handle_exception(e, 'load binance_available_pairs exception')   
                    time.sleep(2)

            
            for symbol in symbols:
                if symbol['status'] == "TRADING" and (symbol['symbol'][-4:] == "USDT" or symbol['symbol'][-3:] == "BTC"):
                    list_to_return.append(symbol['symbol'])
                    
            return list_to_return
        except Exception as e:
            handle_exception(e, 'get_binance_available_pairs exception')

    def run_ws_for_symbols(self):
        try:
            print("Starting new WS connection...")
            sys.stdout.flush()
            ws_url = self.ws_url_base

            for symbol in self.served_symbols:
                ws_url += symbol.lower() + self.ws_url_symbol_addon
            
            ws_url = ws_url[:-1]

            if self.ws_thread != None and self.ws_thread.is_alive() == True: #reinit ws connection, probably some new data appeared
                print("Shutting down existing WS connection...")
                sys.stdout.flush()
                self.ws_connection.close()
                self.ws_thread.join()
             
            
            self.ws_connection.sock = None
            self.ws_connection.url = ws_url

            self.ws_thread = threading.Thread(target=self.ws_connection.run_forever)
            self.ws_thread.start()
            print("New WS connection started!")
            sys.stdout.flush()

        except Exception as e:
            handle_exception(e, "run ws for symbols")


    def update_symbols_with_missing_klines(self): 
        try:

            if len(self.symbol_klines_load_needed) == 0:
                return
        
            sec_after_new_5m = time.time() % kline_sec_duration
            print(sec_after_new_5m, kline_sec_duration-40)

            if sec_after_new_5m > kline_sec_duration-40:
                return

            self.load_symbol_klines('BTCUSDT')
            try:
                btc_pair_index = self.symbol_klines_load_needed.index('BTCUSDT')
                del self.symbol_klines_load_needed[btc_pair_index]
            except:
                pass

            self.construct_btc_time_price_dict()

            for i in range(len(self.symbol_klines_load_needed)):
                sec_after_new_5m = time.time() % kline_sec_duration
                print(sec_after_new_5m, kline_sec_duration-40)

                if sec_after_new_5m > kline_sec_duration-40:
                    return

                self.load_symbol_klines(self.symbol_klines_load_needed[0])
                self.symbol_klines_load_needed.pop(0)


        except Exception as e:
            handle_exception(e, "update_symbols_with_missing_klines")
    
    def load_symbol_klines(self, symbol):
        try:
            print("loading klines for ", symbol)

            klines_num = 48*12 + 1 #because loading 1 kline that is not closed yet, which we should drop, so we'll get 1 less, that causes errors
            klines = self.get_klines((symbol, time.time()-klines_num*kline_sec_duration, 0), "5m")
            
            if klines == None or len(klines) == 0:
                print("No klines was loaded for symbol ", symbol)
                return

            if len(klines) < klines_num: #NONE has no len()
                self.day_ban_set.add(symbol)
                self.banned_changed = True
                print("Symbol appeared less than 48h ago: ", symbol)
                return

            self.symbol_data_vault[symbol] = self.split_loaded_binance_klines(symbol, klines, 0)
        
        except Exception as e:
            handle_exception(e, "load_symbol_klines")

    def get_klines(self, symbol_data, kline_duration, klines_number=0):
    
        """
        symbol_data = (symbol; ts_begin; ts_end)
        if ts_begin = 0, use klines num;
        if ts_begin != 0 and ts_end = 0, ts_end = ts now

        BTCUSDT 3m candle
        Maker is the person who puts an order in the orderbook. Taker is the one that matches an existing order.
        base asset refers to the asset that is the ,quantity of a symbol.
        quote asset refers to the asset that is the price of a symbol

        [
           [
              1543784040000,   // Open time
              '4225.01000000', // Open
              '4226.49000000', // High
              '4213.00000000', // Low
              '4222.37000000', // Close
              '105.46171900',  // Volume             (Amount of BTC bought + sold)
              1543784219999,   // Close time
              '444908.399114', // Base asset volume (Amount of USDT)
              507,             // Number of trades
              '60.09566200',   // Taker "buy" volume in quote asset  (Amount of BTC bought)  
              '253490.3336506',// Taker "buy" volume in base asset (Amount of USDT sold, deal type "market buy BTC")
              '0'              // Ignore.
            ]
        ]
        """
        try:
            kline_duration_val = {"1m":60, "3m":180, "5m":300, "15m":900, "30m":1800, "1h":3600, "2h":7200, "4h":14400, \
                                         "6h":21600, "8h":28800, "12h":43200, "1d":86400, "3d":259200, "1w":604800}
            global_end_timestamp = 0
            global_start_timestamp = 0

            symbol = symbol_data[0]
            get_candles_url_start = "https://api.binance.com/api/v1/klines?symbol="+ symbol + "&interval=" + kline_duration

            if symbol_data[1] != 0:
                global_start_timestamp = int(symbol_data[1])

                if symbol_data[2] != 0:
                    global_end_timestamp = int(symbol_data[2])
                else:
                    global_end_timestamp = int(time.time())

                time_delta = global_end_timestamp - global_start_timestamp

                klines_number = int(time_delta / kline_duration_val[kline_duration])

            returned_array = None

            amount_to_ask = 0
            end_timestamp = 0
            amount_left = klines_number


            if symbol_data[1] == 0:

                while True:

                    if amount_left > 1000:
                        amount_to_ask = 1000
                        amount_left = amount_left - 999
                    else:
                        amount_to_ask = amount_left
                        amount_left = 0

                    get_candles_url = get_candles_url_start +  "&limit=" + str(amount_to_ask)

                    if end_timestamp != 0:
                        get_candles_url += "&endTime=" + str(end_timestamp)

                    parsed_json = None
                    max_retries = 3

                    for i in range(max_retries):
                        try:
                            response = requests.get(get_candles_url, timeout=1)
                            parsed_json = json.loads(response.content)
                            break
                        except Exception as e:
                            print(e)
                            time.sleep(2)

                    if returned_array == None:
                        returned_array = parsed_json
                    else:
                        returned_array = parsed_json[:-1] + returned_array

                    if amount_left == 0 or (amount_to_ask > len(parsed_json)):
                        break
                    else:
                        end_timestamp = returned_array[0][0]
            else:

                start_timestamp = global_start_timestamp * 1000

                while True:

                    if amount_left > 1000:
                        amount_to_ask = 1000
                        amount_left = amount_left - 999
                    else:
                        amount_to_ask = amount_left
                        amount_left = 0

                    get_candles_url = get_candles_url_start + "&limit=" + str(amount_to_ask)
                    get_candles_url += "&startTime=" + str(start_timestamp)

                    if amount_to_ask < 1000:
                        get_candles_url = get_candles_url_start + "&limit=" + str(1000) + "&startTime=" + str(start_timestamp) + "&endTime=" + str(global_end_timestamp*1000)

                    parsed_json = None
                    max_retries = 3

                    for i in range(max_retries):
                        try:
                            response = requests.get(get_candles_url, timeout=1)
                            parsed_json = json.loads(response.content)
                            break
                        except Exception as e:
                            print(e)
                            time.sleep(2)

                    if returned_array == None:
                        returned_array = parsed_json
                    else:
                        returned_array += parsed_json[:-1]

                    if amount_left == 0 or (amount_to_ask > len(parsed_json)):
                        break
                    else:
                        start_timestamp = parsed_json[-1][0]

            return returned_array
        except Exception as e:
            handle_exception(e, 'get_klines')

    def split_loaded_binance_klines(self, symbol, klines, kline_ts_index):
        #timestamp:open:high:low:close total_deals 
        try:
            splited_data = binance_symbol_data()
            splited_data.represented_symbol = symbol

            should_use_btc_price = False

            if symbol[-3:] == "BTC":
                should_use_btc_price = True

            for kline in klines:
                kline_timestamp = int(kline[kline_ts_index]/1000)
                
                if time.time() - kline_timestamp < 300:
                    continue

                splited_data.timestamp.append(kline_timestamp)
                
                splited_data.open.append(float(kline[kline_ts_index+1]))
                splited_data.high.append(float(kline[kline_ts_index+2]))
                splited_data.low.append(float(kline[kline_ts_index+3]))
                splited_data.close.append(float(kline[kline_ts_index+4]))
                
                splited_data.deals_num.append(float(kline[kline_ts_index+8]))
                
                splited_data.total_base_vol.append(float(kline[kline_ts_index+5]))
                splited_data.buy_base_vol.append(float(kline[kline_ts_index+9]))

                if should_use_btc_price == False:
                    splited_data.total_quote_vol.append(float(kline[kline_ts_index+7]))
                    splited_data.buy_quote_vol.append(float(kline[kline_ts_index+10]))
                else:    
                    try:
                        splited_data.total_quote_vol.append(float(kline[kline_ts_index+7]) * self.btc_time_price_dict[kline_timestamp]) 
                        splited_data.buy_quote_vol.append(float(kline[kline_ts_index+10]) * self.btc_time_price_dict[kline_timestamp])
                    except Exception as e:
                        print(e, "btc_time_price_dict error str 568", time.time(), splited_data.timestamp[-1])
            return splited_data
        except Exception as e:
            handle_exception(e, "split loaded")
        
    def construct_btc_time_price_dict(self):
        symbol_data = self.symbol_data_vault["BTCUSDT"]
        
        for i in range(len(symbol_data.timestamp)):
            self.btc_time_price_dict[symbol_data.timestamp[i]] = symbol_data.close[i]


    def spin_that_shit(self):
        try:
            checks_count = 0
            skip_iteration = False
            daily_banned_symbol_reseted = True
            
            while True: 
                checks_count += 1

                print("main func new cycle ", time.time())

                if time.time() % (3600*24) < 3600 and daily_banned_symbol_reseted == False:
                    self.day_ban_set = set()
                    daily_banned_symbol_reseted = True
                else:
                    daily_banned_symbol_reseted = False

                global full_restart_request

                if full_restart_request == True:
                    print("full restart request found")
                    sys.stdout.flush()
                    self.restart_ws_connection()
                    full_restart_request = False
                    skip_iteration = True

                if self.banned_changed == True or (skip_iteration != True and (checks_count == 12 or self.ws_thread == None)):
                    checks_count = 0
                    self.uptime_hours += 1
                    self.banned_changed = False
                    print("getting available pairs")

                    fresh_symbols = set(self.get_binance_available_pairs()) #["POEBTC", "ETHBTC"]
                    fresh_symbols = fresh_symbols.difference(self.day_ban_set)

                    dropped_symbols = self.served_symbols.difference(fresh_symbols)

                    if(fresh_symbols != self.served_symbols):
                        print("fresh_symbols is different from served_symbols")
                        for symbol in dropped_symbols:
                            if symbol in self.symbol_data_vault: 
                                del self.symbol_data_vault[symbol]
                            print("dropping ", symbol)

                        self.served_symbols = fresh_symbols
                        self.restart_ws_connection()
                        skip_iteration = True


                if skip_iteration != True and self.uptime_hours == 20:
                    print("Resetting connection and updating blacklist after 20h of updates")
                    sys.stdout.flush()
                    self.load_blacklisted_symbols()
                    self.restart_ws_connection()

                self.update_symbols_with_missing_klines()
                skip_iteration = False

                secs_passed_since_new_5m = int(time.time()) % kline_sec_duration

                if int(time.time()) % 900 < 600:
                    print("Main thread sleep for ", kline_sec_duration + 15 - secs_passed_since_new_5m, "main func run took ", secs_passed_since_new_5m)
                    time.sleep(kline_sec_duration + 15 - secs_passed_since_new_5m)
                else:
                    print("Main thread sleep for ", kline_sec_duration - secs_passed_since_new_5m - 2, "main func run took ", secs_passed_since_new_5m)
                    time.sleep(kline_sec_duration - secs_passed_since_new_5m - 2)

        except Exception as e:
            handle_exception(e, "main run func")

    def restart_ws_connection(self):
        print("restarting wss connection")
        self.uptime_hours = 0
        current_timestamp = int(time.time())
        secs_passed_since_new_5m = current_timestamp % kline_sec_duration
        
        if secs_passed_since_new_5m > 250:
            print("sleeping ", 310 - secs_passed_since_new_5m, "seconds before creating new one")
            time.sleep(310 - secs_passed_since_new_5m)
        
        self.run_ws_for_symbols()

        print("return after ws started")



def on_message(ws, message):
    try:
        current_timestamp = time.time()

        parsed_json = json.loads(message)

        new_kline_ts_open = parsed_json["data"]["k"]["t"]
        new_kline_symbol = parsed_json["data"]["s"]
        kline_is_closed = parsed_json["data"]["k"]["x"]

        global data_manager

        if new_kline_symbol == "BTCUSDT":
            data_manager.last_btc_price = float(parsed_json["data"]["k"]["c"])

        if current_timestamp % 900 > 600:
            on_open_kline_update(parsed_json, data_manager)
        
        if(kline_is_closed == True):
            on_closed_kline_update(parsed_json)

        # print(new_kline_symbol, parsed_json["data"]["E"], " update: ", time.time(), parsed_json["data"]["k"]["q"], parsed_json["data"]["k"]["t"], parsed_json["data"]["k"]["x"])
                
    except Exception as e:
        handle_exception(e, "on message")

def on_closed_kline_update(parsed_json):
    try:
        new_kline_ts_open = int(parsed_json["data"]["k"]["t"])/1000
        new_kline_symbol = parsed_json["data"]["s"]
        kline_is_closed = parsed_json["data"]["k"]["x"]

        global data_manager

        if new_kline_symbol == 'BTCUSDT':
            data_manager.btc_time_price_dict[new_kline_ts_open] = float(parsed_json["data"]["k"]["c"])
        

        try:
            symbol_data = data_manager.symbol_data_vault[new_kline_symbol]
        except:
            if new_kline_symbol not in data_manager.symbol_klines_load_needed:
                print("not processed yet symbol: ", new_kline_symbol)
                data_manager.symbol_klines_load_needed.append(new_kline_symbol)
            return

        if symbol_data == None:
            print("None symbol data: ", new_kline_symbol, symbol_data)

        if int(new_kline_ts_open) - symbol_data.timestamp[-1] > kline_sec_duration: #in milliseconds
            if new_kline_symbol not in data_manager.symbol_klines_load_needed: 
                print(new_kline_symbol, ': delta with last present kline is ', (new_kline_ts_open) - symbol_data.timestamp[-1], 
                                        'last present ts open: ', symbol_data.timestamp[-1], 
                                        'new kline ts open: ', new_kline_ts_open)
                
                data_manager.symbol_klines_load_needed.append(new_kline_symbol)
            
            return

        symbol_data.current_kline_update_json = parsed_json
        symbol_data.last_btc_price = data_manager.last_btc_price

        symbol_data.process_new_closed_kline(parsed_json)

    except Exception as e:
        handle_exception(e, "on_closed kline")

def on_open_kline_update(parsed_json, data_manager):
    try:
        new_kline_symbol = parsed_json["data"]["s"]
                
        if new_kline_symbol not in data_manager.symbol_data_vault.keys():
            if new_kline_symbol not in data_manager.symbol_klines_load_needed:
                print("not processed yet symbol: ", new_kline_symbol)
                data_manager.symbol_klines_load_needed.append(new_kline_symbol)
            return

        symbol_data = data_manager.symbol_data_vault[new_kline_symbol]
        symbol_data.last_btc_price = data_manager.last_btc_price
        symbol_data.current_kline_update_json = parsed_json
    except Exception as e:
        handle_exception(e, "on open kline")

def on_error(ws, error):
    print(error)
    sys.stdout.flush()
    try:
        ws.close()
    except:
        ws.sock.close()
        ws.keep_running = False

    global full_restart_request 
    full_restart_request = True

def on_close(ws):
    print("### closed ###")
    sys.stdout.flush()


def main():
    global data_manager

    data_manager = binance_ws_data_collector()
    data_manager.spin_that_shit()

if __name__ == "__main__":
    main()


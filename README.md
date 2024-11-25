import requests
from NseIndia import NSE
import xlwings as xw
import os, sys,json,datetime,time
import pandas as pd
import dateutil.parser
import numpy as np
from kiteconnect import KiteConnect, KiteTicker

def get_login_credentials():
    global login_credential

    def login_credentials():
        print("......Enter your Zerodha Login Credentials ......")
        login_credential = {
            "api_key": input(" Enter API Key: ").strip(),
            "api_secret": input("Enter API Secret: ").strip()
        }
        if input("Press Y to save Login credential and any key to bypass: ").strip().upper() == "Y":
            with open("zerodha_login_details.json", "w") as f:
                json.dump(login_credential, f)
            print("Data Saved ....")
        else:
            print("Data Save canceled !!!!!")

    while True:
        try:
            with open("zerodha_login_details.json", "r") as f:
                login_credential = json.load(f)
            break
        except FileNotFoundError:
            login_credentials()
    return login_credential

def get_access_token():
    global login_credential, access_token

    print("Trying Log In....")
    if login_credential["api_key"] == "Monu":
        print("Login URL: http://kite.zerodha.com (Don't login Anywhere else after this, Instead of mobile App.)")
        access_token = input("Login and Enter your 'enctoken' here: ")
    else:
        kite = KiteConnect(api_key=login_credential["api_key"])
        print("Login URL:", kite.login_url())
        request_tkn = input("Login and enter your 'request_token' here: ")
        try:
            access_token = kite.generate_session(request_tkn, api_secret=login_credential["api_secret"])['access_token']
        except Exception as e:
            print(f"Login Failed: {e}!!!!!!")

    os.makedirs("AccessToken", exist_ok=True)
    with open(f"AccessToken/{datetime.datetime.now().date()}.json", 'w') as f:
        json.dump(access_token, f)

    while True:
        if os.path.exists(f"AccessToken/{datetime.datetime.now().date()}.json"):
            with open(f"AccessToken/{datetime.datetime.now().date()}.json", 'r') as f:
                access_token = json.load(f)
            break
        else:
            login()
    return access_token

def login():
    get_login_credentials()
    get_access_token()

def get_object():
    global kite, login_credential, access_token, user_id

    try:
        if login_credential["api_key"] == "Monu":
            class KiteApp:
                PRODUCT_MIS = "MIS"
                PRODUCT_CNC = "CNC"
                PRODUCT_NRML = "NRML"
                PRODUCT_CO = "CO"
                
                ORDER_TYPE_MARKET = "MARKET"
                ORDER_TYPE_LIMIT = "LIMIT"
                ORDER_TYPE_SL_M = "SL-M"
                ORDER_TYPE_SL = "SL"

                VARIETY_REGULAR = "regular"
                VARIETY_CO = "co"
                VARIETY_AMO = "amo"

                TRANSACTION_TYPE_BUY = "BUY"
                TRANSACTION_TYPE_SELL = "SELL"

                VALIDITY_DAY = "DAY"
                VALIDITY_IOC = "IOC"

                EXCHANGE_NSE = "NSE"
                EXCHANGE_BSE = "BSE"
                EXCHANGE_NFO = "NFO"
                EXCHANGE_CDS = "CDS"
                EXCHANGE_BFO = "BFO"
                EXCHANGE_MCX = "MCX"

                def __init__(self, enctoken):
                    self.enctoken = enctoken
                    self.headers = {"Authorization": f"enctoken {self.enctoken}"}
                    self.session = requests.Session()
                    self.root_url = "https://kite.zerodha.com/oms"
                    self.session.get(self.root_url, headers=self.headers)

                def instruments(self, exchange=None):
                    data = self.session.get("https://api.kite.trade/instruments").text.split("\n")
                    Exchange = []
                    for i in data[1:-1]:
                        row = i.split(",")
                        if exchange is None or exchange == row[11]:
                            Exchange.append({
                                'instrument_token': int(row[0]),
                                'exchange_token': row[1],
                                'tradingsymbol': row[2],
                                'name': row[3][1:-1],
                                'last_price': float(row[4]),
                                'expiry': dateutil.parser.parse(row[5]).date() if row[5] != "" else None,
                                'strike': float(row[6]),
                                'tick_size': float(row[7]),
                                'lot_size': int(row[8]),
                                'instrument_type': row[9],
                                'segment': row[10],
                                'exchange': row[11]
                            })
                    return Exchange

                def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
                    params = {
                        "from": from_date,
                        "to": to_date,
                        "interval": interval,
                        "continuous": 1 if continuous else 0,
                        "oi": 1 if oi else 0
                    }
                    lst = self.session.get(
                        f"{self.root_url}/instruments/historical/{instrument_token}/{interval}", 
                        params=params,
                        headers=self.headers
                    ).json()["data"]["candles"]
                    records = []
                    for i in lst:
                        record = {
                            "date": dateutil.parser.parse(i[0]),
                            "open": i[1],
                            "high": i[2],
                            "low": i[3],
                            "close": i[4],
                            "volume": i[5],
                        }
                        if len(i) == 7:
                            record["oi"] = i[6]
                        records.append(record)
                    return records

                def margins(self):
                    return self.session.get(f"{self.root_url}/user/margins", headers=self.headers).json()["data"]

                def profile(self):
                    return self.session.get(f"{self.root_url}/user/profile", headers=self.headers).json()["data"]

                def orders(self):
                    return self.session.get(f"{self.root_url}/user/orders", headers=self.headers).json()["data"]

                def positions(self):
                    return self.session.get(f"{self.root_url}/user/positions", headers=self.headers).json()["data"]

                def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type,
                                price=None, validity=None, disclosed_quantity=None, trigger_price=None, 
                                squareoff=None, stoploss=None, trailing_stoploss=None, tag=None):
                    params = locals()
                    del params["self"]
                    for k in list(params.keys()):
                        if params[k] is None:
                            del params[k]
                    return self.session.post(
                        f"{self.root_url}/orders/{variety}", data=params, headers=self.headers
                    ).json()["data"]["order_id"]

                def modify_order(self, variety, order_id, parent_order_id=None, quantity=None, price=None,
                                 order_type=None, trigger_price=None, validity=None, disclosed_quantity=None):
                    params = locals()
                    del params["self"]
                    for k in list(params.keys()):
                        if params[k] is None:
                            del params[k]
                    return self.session.put(
                        f"{self.root_url}/orders/{variety}/{order_id}", data=params, headers=self.headers
                    ).json()["data"]["order_id"]

                def cancel_order(self, variety, order_id, parent_order_id=None):
                    return self.session.delete(
                        f"{self.root_url}/orders/{variety}/{order_id}",
                        data={"parent_order_id": parent_order_id} if parent_order_id else {},
                        headers=self.headers
                    ).json()["data"]["order_id"]

            kite = KiteApp(enctoken=access_token)
        else:
            kite = KiteConnect(api_key=login_credential["api_key"], access_token=access_token)

        user_id = kite.profile()["user_id"]
        print(f"Logged In : {user_id}")
    except Exception as e:
        print(f"Login Error: {e}!!!!!")
        os.remove(f"AccessToken/{datetime.datetime.now().date()}.json") if os.path.exists(
            f"AccessToken/{datetime.datetime.now().date()}.json") else None
        time.sleep(5)
        sys.exit()

def start_websocket():
    global login_credential, access_token, user_id, kws, tick_data, symbol_token, token_symbol

    access_token = access_token + "&user_id=" + user_id if login_credential["api_key"] == "Monu" else access_token
    kws = KiteTicker(api_key=login_credential["api_key"], access_token=access_token)

    tick_data = {}
    token_symbol = {}

    def on_ticks(ws, ticks):
        for i in ticks:
            tick_data[token_symbol[i["instrument_token"]]] = i

    kws.on_ticks = on_ticks
    kws.connect(threaded=True)
    while not kws.is_connected():
        time.sleep(1)
    print("WebSocket: Connected")

get_login_credentials()
get_access_token()
get_object()
start_websocket()


nse = NSE()

if not os.path.exists("Bablu.xlsx"):
    try:
        wb = xw.Book()
        wb.sheets.add("OptionChain")
        wb.save("Bablu.xlsx")
        wb.close()
    except Exception as e:
        print(f"Error Creating Excel File: {e}")
        sys.exit()

wb = xw.Book("Bablu.xlsx")
oc = wb.sheets("OptionChain")
oc.range("a:b").value = oc.range("d6:e19").value = oc.range("g1:v4000").value = None
df = pd.DataFrame({"FNO Symbol": ["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"] + nse.equity_market_data("NIFTY 50", symbol_list=True)})
df = df.set_index("FNO Symbol", drop=True)
oc.range("a1").value = df

oc.range("d2").value, oc.range("d3").value = "Symbol == >>", "Expiry == >>"
print("Excel: Started")

pre_oc_symbol = pre_oc_expiry = ""
expiries_list = []

while True:
    time.sleep(5)
    oc_symbol, oc_expiry = oc.range("e2").value, oc.range("e3").value
    if pre_oc_symbol != oc_symbol or pre_oc_expiry != oc_expiry:
        oc.range("g1:v4000").value = None
        if pre_oc_symbol != oc_symbol:
            oc.range("b:b").value = oc.range("d6:e19").value = True
            expiries_list = []
        pre_oc_symbol = oc_symbol
        pre_oc_expiry = oc_expiry
        
    if oc_symbol is not None:
        indices = True if oc_symbol == "NIFTY" or oc_symbol == "BANKNIFTY" or oc_symbol == "FINNIFTY" or oc_symbol == "MIDCPNIFTY" else False
        try:
            if not expiries_list:
                for i in list(nse.option_data(oc_symbol, indices)["expiryDate"]):
                    if dateutil.parser.parse(i).date() not in expiries_list:
                        expiries_list.append(dateutil.parser.parse(i).date())
                df = pd.DataFrame({"Expiry Date": [str(i) for i in sorted(expiries_list)]})
                df = df.set_index("Expiry Date", drop=True)
                oc.range("b1").value = df

            df = nse.option_data(oc_symbol, indices)
            df["expiryDate"] = df["expiryDate"].apply(lambda x: dateutil.parser.parse(x))
            df = df[df["expiryDate"] == oc_expiry]
            timestamp = list(df["timestamp"])[0]
            underlying_price = list(df["underlyingValue"])[0]

            ce_df = df[df["instrumentType"] == "CE"]
            ce_df = ce_df[["openInterest","changeinOpenInterest","totalTradedVolume", "change", 'impliedVolatility',"lastPrice","strikePrice"]]
            ce_df = ce_df.rename(columns={"openInterest": "CE OI", "changeinOpenInterest": "CE Change in OI", 'impliedVolatility': "CE IV", "lastPrice": "CE LTP", "change": "CE LTP Change", "totalTradedVolume": "CE Volume"})
            ce_df.set_index("strikePrice", drop=True, inplace=True)
            ce_df["Strike"] = ce_df.index

            pe_df = df[df["instrumentType"] == "PE"]
            pe_df = pe_df[["strikePrice","lastPrice",'impliedVolatility',"change","totalTradedVolume","changeinOpenInterest","openInterest"]]
            pe_df = pe_df.rename(columns={"openInterest": "PE OI", "changeinOpenInterest": "PE Change in OI", 'impliedVolatility': "PE IV", "lastPrice": "PE LTP", "change": "PE LTP Change", "totalTradedVolume": "PE Volume"})
            pe_df.set_index("strikePrice", drop=True, inplace=True)

            df = pd.concat([ce_df, pe_df], axis=1).sort_index()
            df = df.replace(np.nan, 0)
            df["Strike"] = df.index
            df.index = [np.nan] * len(df)

            oc.range("d6").value = [["TimeStamp", timestamp],
                                    ["Spot LTP", underlying_price],
                                    ["Total Call OI", sum(list(df["CE OI"]))],
                                    ["Total Put OI", sum(list(df["PE OI"]))],
                                    ["Total CE Change in OI", sum(list(df["CE Change in OI"]))],
                                    ["Total PE Change in OI", sum(list(df["PE Change in OI"]))],
                                    ["", ""],
                                    ["Max Call OI", max(list(df["CE OI"]))],
                                    ["Max Put OI", max(list(df["PE OI"]))],
                                    ["Max Call OI Strike", list(df[df["CE OI"] == max(list(df["CE OI"]))]["Strike"])[0]],
                                    ["Max Put OI Strike", list(df[df["PE OI"] == max(list(df["PE OI"]))]["Strike"])[0]],
                                    ["", ""],
                                    ["Max Call Change in OI", max(list(df["CE Change in OI"]))],
                                    ["Max Put Change in OI", max(list(df["PE Change in OI"]))],
                                    ["Max Call Change in OI Strike",
                                    list(df[df["CE Change in OI"] == max(list(df["CE Change in OI"]))]["Strike"])[0]],
                                    ["Max Put Change in OI Strike",
                                    list(df[df["PE Change in OI"] == max(list(df["PE Change in OI"]))]["Strike"])[0]],
                                    ["Max Call Volume Strike",
                                    list(df[df["CE Volume"] == max(list(df["CE Volume"]))]["Strike"])[0]],
                                    ["Max Put Volume Strike",
                                    list(df[df["PE Volume"] == max(list(df["PE Volume"]))]["Strike"])[0]],
                                   ]

            oc.range("g1").value = df
            
        except :
            pass

    

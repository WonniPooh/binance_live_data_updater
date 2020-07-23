# binance live DataUpdater

This code is created to update all Binance assets data, with quote asset of either USDT or BTC.

Program was originally designed to provide new and updated data for 15 minute kline 2 seconds before it closes; 
Mentioned functionality is implemented but not used in this code - some of narrow-focused code was dropped before publishing. 
Stated task was done by losing/skipping information of last 2 seconds of kline data before it closes;
That skipped data is be taken into account after kline closes, to avoid error accumulation in stored klines history.

Class "binance_symbol_data" stores exactly 48h of 5 minutes klines, all data is updated with a websocket connection, after loads.
Implemented mechanism that allows to start data update N seconds before latest 5m kline closes. That is possible
because code stores every 2 seconds assets kline state update in "current_kline_update_json" field of "binance_symbol_data" class;
That stored data can be applied/inserted into  existing history by calling 'process_unclosed_kline' function;
After closed kline data is received, it automaticaly replaces unclosed kline data in history with completed kline data, 
if that fuctionality was used; Else nothing is done.

WebSocket connection is running in separate thread.

Active assets list update happens every hour.

WebSocket connection resets after 20h straight uptime.

If asset has less thatn 48h of traiding history its updates is suspended for 24 hours, before next check.

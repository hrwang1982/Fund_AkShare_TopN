# Fund_AkShare_TopN

一个根据涨幅动量进行基金选取的程序，包含了：
 #1. 用于初始化数据库，并获取初始数据；
fund_AkShare_Init-getdata.py 

#2. 每日用于更新数据，并增量添加入数据库
fund_AkShare_update-data.py   

#3. 根据最新的数据，重新计算（如连续6周top500），根据模型（比如最近3周的top500求交集）基金筛选，生成页面可以查看分析结果
fund_AkShare_calculate.py    

#4. 可用于自己选择的模型，进行回测
fund_backtest.py              

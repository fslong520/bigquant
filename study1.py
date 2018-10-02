class Choose_stock(object):
    # 声明周期并获取所有股票代码：
    def __init__(self, start_date='2017-01-01', end_date='2018-09-28'):
        self.start_date = start_date
        self.end_date = end_date
        self.instruments = self.get_all_stock()

    # 获取所有股票：

    def get_all_stock(self):
        return D.instruments(start_date=self.start_date, end_date=self.end_date)

    # 获取沪深300成分股的股票数据：

    def get_df300(self):
        df = D.history_data(self.instruments, self.start_date,
                            self.end_date, ['in_csi300'])
        instruments = list(set(df[df['in_csi300'] == 1]['instrument']))
        if instruments:
            print('沪深指数成分股预览10支股票：\n', instruments[:10])
            return instruments

    # 获取某个行业的股票数据，以国防军工行业股票为例(有既定的板块代码)：

    def get_df_hy(self, ｈy='industry_sw_level1', hy_num=650000):
        df = D.history_data(
            self.instruments, self.start_date, self.end_date, [hy])
        instruments = list(set(df[df[hy] == hy_num]['instrument']))
        if instruments:
            print(D.history_data(instruments, self.start_date,
                                 self.end_date, ['company_name']).head())
            return instruments

    # 获取某个概念、板块的股票（无既定的板块代码，风格概念的关键字搜索吧）：

    def get_df_concept(self, concept='人工智能'):
        # concept是股票的概念、板块
        df = D.history_data(self.instruments, self.start_date,
                            self.end_date, ['concept']).dropna()
        df['cp'] = df['concept'].map(lambda x: concept in x)
        st = list(df[df['cp'] == True]['instrument'])
        if st:
            print(D.history_data(st, self.start_date,
                                 self.end_date, ['name', 'concept']).head())
            return st

    # 获取次新股（上市90天以内，返回值是一个列表，选中了设定的start_date和end_date之间每日的值，如果只想选择某天的，直接把两个日期选成一天就可以）：

    def get_df_cx(self):
        # self.start_date='2018-09-28'
        df = D.history_data(self.instruments, self.start_date,
                            self.end_date, ['list_date', 'list_board'])
        from datetime import timedelta
        return df[(df['list_board'] == '创业板') & (df['date'] <= df['list_date']+timedelta(days=90))]

    # 根据一些财务指标选股：

    def get_df_cw(self):
        # 市盈率小于15倍、市净率小于1.5倍、市销率大于0.4、资产周转率大于0.4、市值最小的10只股票
        df = D.history_data(self.instruments, self.start_date, self.end_date, [
                            'pe_ttm', 'pb_lf', 'ps_ttm', 'market_cap', 'amount'])  # 前面三个与上面条件中的指标一一对应，具体参见api文档
        # 通过financial获取财务指标：
        financial = D.features(self.instruments, self.start_date, self.end_date, [
                               'fs_operating_revenue_ttm_0', 'fs_current_assets_0', 'fs_non_current_assets_0'])
        # 总资产=流动资产+非流动资产：
        financial['total_assets'] = financial['fs_current_assets_0'] + \
            financial['fs_non_current_assets_0']
        # 资产周转率=营业收入/总资产：
        financial['asset_turnover'] = financial['fs_operating_revenue_ttm_0'] / \
            financial['total_assets']
        financial_data = financial[['date', 'instrument', 'asset_turnover']]
        # 两个DataFrame:历史数据、财务数据 合并：
        result = df.merge(
            financial_data, on=['date', 'instrument'], how='outer')
        # 按照选股法则选出股票并按照时间分组：
        daily_buy_stock = result.groupby('date').apply(lambda df: list(df[(df['ps_ttm'] > 0.4) & (df['pb_lf'] < 8) & (df['asset_turnover'] > 0.4) & (
            df['amount'] > 100) & (df['pe_ttm'] < 15) & (df['pe_ttm'] > 0)].sort_values('market_cap')['instrument'])[:10])
        return daily_buy_stock

    # 通过技术层面选股：
    # 7日均线上穿63日均线、收盘价突破ATR上轨：

    def get_df_js1(self):
        import talib
        import numpy
        from numpy import float
        df = D.history_data(self.instruments, self.start_date, self.end_date, fields=[
                            'open', 'close', 'low', 'high'])

        def seek_stocks(df):
            df['ma_7'] = talib.SMA(df.close.map(float).values, 7)
            df['ma_63'] = talib.SMA(df.close.map(float).values, 63)
            try:
                df['atr'] = talib.ATR(df.high.map(float).values, df.low.map(
                    float).values, df.close.map(float).value, 14)
            except:
                df['atr'] = numpy.nan
            df['upper_line'] = df.close.rolling(15).mean()+df['atr']
            return df[(df['ma_7'] > df['ma_63']) & (df['close'] >= df['upper_line'])].drop('instrument', axis=1)
        result = df.groupby('instrument').apply(seek_stocks).reset_index()
        daily_buy_stock = result.groupby('date').apply(
            lambda df: list(df['instrument']))
        return daily_buy_stock

    # 股价创60日最高点、3日线上穿5日均线，5日均线上穿10日均线、当日成交额是昨日成交额的1.4倍、macd柱状图处于红色区域

    def get_df_js2(self):
        import talib
        import numpy
        df = D.history_data(self.instruments, self.start_date,
                            self.end_date, ['close', 'amount', 'high'])

        def seek_stocks(df):
            df['highest_60'] = df['high'].rolling(60).max()  # 计算60天最高点
            df['ma3_cross_ma5'] = df['close'].rolling(
                3).mean() - df['close'].rolling(5).mean() > 0  # 3日均线上穿5日均线
            df['ma5_cross_ma10'] = df['close'].rolling(
                5).mean() - df['close'].rolling(10).mean() > 0  # 5日均线上穿10日均线
            df['amount_cond'] = df['amount'] / \
                df['amount'].shift(1) >= 1.4   # 当日成交量是前一日的1.4倍
            prices = df['close'].map(numpy.float)  # 收盘价转化成float格式
            # macd:diff线 信号线:dea 柱状图：diff-dea
            macd, signal, hist = talib.MACD(
                numpy.array(prices), 12, 26, 9)  # 计算macd各个指标
            # 该列是布尔型变量，表明是否是60日最高点
            df['is_highest'] = df['close'] == df['highest_60']
            df['hist_is_red'] = hist > 0   # macd柱状图是否在红色区域
            return df

        managed_df = df.groupby('instrument').apply(seek_stocks).reset_index()
        # 按照要求选股：
        result = managed_df[
            (managed_df['hist_is_red']) &   # macd在红色区域
            (managed_df['is_highest']) &   # 是60日最高点
            (managed_df['ma3_cross_ma5']) &  # 3日均线上穿5日均线
            (managed_df['ma5_cross_ma10']) &   # 5日均线上穿10日均线
            (managed_df['amount_cond'])]   # 满足成交量是前一日的1.4倍的条件

        # 整理出每日符合买入条件的列表
        daily_buy_stock = result.groupby('date').apply(lambda df: list(
            df.instrument)).reset_index().rename(columns={0: 'stocks'})
        daily_buy_stock.head()
        return daily_buy_stock


choose_stock = Choose_stock()
choose_stock.get_df_js2()

from pytrends.request import TrendReq
import matplotlib.pyplot as plt

pygt = TrendReq()
pygt.build_payload(kw_list=['bitcoin'])

interest_over_time_df = pygt.interest_over_time()
del interest_over_time_df['isPartial']
plot = plt.plot(interest_over_time_df)
plt.show()
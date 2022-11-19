"""

    """

import requests
import pandas as pd

##
id = 48990026850202503
url = f"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={id}&t=ph&a=0"

r = requests.get(url)

##
df = pd.DataFrame(r.text.split(';'), columns=[0])
df = df[0].str.split(',', expand=True)

##
url = f"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={id}&t=ph&a=1"

r = requests.get(url)

##
df1 = pd.DataFrame(r.text.split(';'), columns=[0])
df1 = df1[0].str.split(',', expand=True)


##
df2 = df.eq(df1)

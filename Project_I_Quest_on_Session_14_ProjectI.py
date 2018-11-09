#Project I Question Session 14: Project I

#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np


# In[3]:


df1=pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv') 


# In[4]:


df=pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')


# In[5]:


df.head()


# In[6]:


df1.head()


# 1.Get the metadata from the above files.

# In[61]:


df.dtypes


# In[62]:


df1.dtypes


# 2.Get the row names from the above files

# In[21]:


df.index


# In[24]:


(df.index).values


# In[25]:


(df1.index).values


# 3. Change the column name from any of the above file.

# In[26]:


df.columns.values[3]


# In[27]:


df.columns.values[3]="region HOW"


# In[28]:


df.columns.values[3]


# 4. Change the Column name from any of the above file and store the cahnges made permanentely.

# In[30]:


df.rename(columns=lambda x: x.replace('Year','year'),inplace=True)


# In[31]:


df.head()


# In[32]:


df.columns.values[[2,3]]=['CalYear','Region HWO']


# In[33]:


df.head()


# In[36]:


df.head()


# In[39]:


df.sort_values(['Display Value'],ascending=True)


# 7. Arrange multiple column values in ascending order.

# In[40]:


df.sort_values(['Display Value','Sex'],ascending=True)


# 8. Make country as the first Column of the datafarmae

# 9.Get the column array using a variable

# In[68]:


get_Column_Sex=df["Sex"].values
get_Column_Sex


# 10. Get the subset rows 11,24,37 

# In[70]:


df.iloc[[11,24,37],:]


# 11. Get the subset rows excluding 5,12,23, and 56  

# In[79]:


new_df=df[(df.index !=5) & (df.index !=12) & (df.index !=23) & (df.index !=56)]


# In[80]:


new_df.head(6)


# 12.Join users to transactions , keeping all rows from transactions and only matching rows from users(left join)

# In[88]:


users=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')


# In[89]:


users.head()
sessions.head()
products.head()
transactions.head()


# In[91]:


users.head()


# In[6]:


import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import sqlite3 as db
from pandasql import *

# In[7]:


get_ipython().system('pip install pandasql')


# In[8]:


get_ipython().system('python -m pip install --upgrade pip')


# In[12]:


pysqldf = lambda q: sqldf(q, globals())


# In[13]:


q="""select * from transactions t left join users u on t.userid=u.userid;"""


# In[14]:


df=pysqldf(q)
df


# 13. Which transaction have a UserID not in users ?

# In[151]:


df.loc[df.User.isnull(),'TransactionID']


# 14. Join users to transaction , keeping only rows from transactions and users that match via UserID.

# In[160]:


pysqldf = lambda p: sqldf(p, globals())


# In[153]:


p="""select * from transactions t inner join users u on t.userid=u.userid;"""


# In[154]:


df=pysqldf(p)
df


# 15. Join users to transaction,diplaying all matchingrows AND all non-matching rows 

# In[1]:


pysqldf = lambda r: sqldf(r, globals())


# In[2]:


r="""select * from transactions t full outer join users u on t.userid=u.userid;"""


# In[3]:


df=pysqldf(r)
df



# 15. Join users to transactions, displaying all matching rows AND all non-matching rows
# (full outer join) 

# In[156]:


pysqldf = lambda q: sqldf(q, globals())


# In[157]:


q="""select * from transactions t left join users u on t.userid=u.userid;"""


# In[158]:


df=pysqldf(q)
df


# 16. Determine which sessions occurred on the same day each user registered

# In[14]:


pysqldf = lambda r: sqldf(r, globals())


# In[15]:


r="""select u.*,s.SessionID,s.SessionDate from users u inner join sessions s on u.userid=s.userid where u.Registered=s.SessionDate ;"""


# In[16]:


df=pysqldf(r)
df


# 17. Build a dataset with every possible (UserID, ProductID) pair (cross join)

# In[18]:


pysqldf = lambda s: sqldf(s, globals())
s="""select u.UserId,p.ProductID from users u cross join products p ;"""
df=pysqldf(s)
df


# 18.Determine how much quntity of each product was purchased by each user 

# In[147]:


pysqldf = lambda t: sqldf(t, globals())
t="""select u.UserId,t.ProductID,sum(Quantity) as Quantity from users u cross join  transactions t  on u.UserId=t.UserId GROUP BY u.UserId,t.ProductID 
"""
df=pysqldf(t)
df


# 19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)

# In[163]:


pysqldf = lambda v: sqldf(v, globals())
v="""SELECT X.TransactionID AS TransactionID_X,X.TransactionDate as TransactionDate_X,X.UserId as UserId,X.ProductID as ProductID_X, X.Quantity as Quantity_X,Y.TransactionID AS TransactionID_Y,Y.TransactionDate as TransactionDate_Y,Y.ProductID as ProductID_Y, Y.Quantity as Quantity_Y FROM transactions X, transactions Y WHERE X.UserId = Y.UserId ORDER BY X.UserId DESC
"""
df=pysqldf(v)
df


# 20.Join each user to his/her first occuring transaction in the transactions table

# In[223]:


pysqldf = lambda w: sqldf(w, globals())
w="""select u.*,t.TransactionID as TransactionID,MIN(t.TransactionDate) as TransactionDate,t.ProductID as ProductID,t.Quantity as Quantity from users u left join transactions t on u.UserID=t.UserID group by u.UserID ORDER BY t.TransactionDate DESC"""
df=pysqldf(w)
df


# In[ ]:





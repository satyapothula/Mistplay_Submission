#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
data=pd.read_json('data.json',lines=True) #reading the json file
df = pd.DataFrame(data)


# In[2]:


df


# In[3]:


df1=df.drop_duplicates(
  subset = ['id','created_at'],
  keep = 'last').reset_index(drop = True) #Removing the duplicates over the columns "id" and "created_at"


# In[4]:


df1


# In[5]:


df1["sub_group_rank"]=df1.groupby("age_group")['user_score'].rank(ascending=0)
#Computing the rank for each user's score with in age group. i.e more user score less the rank number


# In[6]:


df1


# In[7]:


df2=df1.explode('widget_list') # flattening the list items i.e. each item in the list is put into its own row using explode.


# In[8]:


df2


# In[9]:


df2[["no_ele","amount","name"]]=df2['widget_list'].apply(pd.Series)
#extracting the values in the JSON elements into their own columns.


# In[10]:


df2


# In[11]:


df2['email_anon'] = df2['email'].astype('category').cat.codes
#anonymize the column emailand output the anonymized version in a new column email_anon


# In[12]:


df2


# In[13]:


df3=data.groupby('location')['id'].apply(list).to_json('inverted_index.json')

#Creating new df for each country list of all the ids belong to that country


# In[14]:


inverted_index=pd.read_json('inverted_index.json',lines=True)
new_df = pd.DataFrame(inverted_index)


# In[15]:


new_df


# In[16]:


import pyarrow.parquet as pq
import pyarrow as pa


# In[17]:


table = pa.Table.from_pandas(df2)
pq.write_table(table, 'op_data.parquet')
#writing the data as parquet file


# In[18]:


table = pa.Table.from_pandas(new_df)
pq.write_table(table, 'op1_data.parquet')


# In[19]:


pd.read_parquet('op_data.parquet', engine='pyarrow')


# In[20]:


pd.read_parquet('op1_data.parquet', engine='pyarrow')


# In[ ]:





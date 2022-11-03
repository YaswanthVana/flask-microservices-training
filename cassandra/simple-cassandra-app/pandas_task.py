import pandas
import matplotlib.pyplot as plt
from sqlalchemy import create_engine


engine = create_engine("cassandra:///?Database=user_management_service&;Port=9042&Server=localhost")

df = pandas.read_sql("SELECT * FROM user_management_service.users ", engine)

df.head()
# Nosferatu
(this project is for learning purposes and still under heavy development and breaking changes)

Access your postgres db,tables,columns as objects

e.g:

```python
from nosferatu import Nosferatu

con = {'host': localhost, 'database': 'your_db_name'}
db = Nosferatu.connect(con)
# print list of tables that exists in the db
for t in db.tables:
    print(t)

# assuming there is a table called posts
db.posts.inspect() # retrieves list of columns
# id
# title
# body
# created_at

# retreive list of posts
posts = db.posts.filter("title LIKE '%hello%'").all()
for p in posts:
    print(p.title, p.created_at)
```